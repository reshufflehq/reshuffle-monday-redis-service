import { RedisConnector } from 'reshuffle-redis-connector'
import { MondayConnector } from 'reshuffle-monday-connector'
import { BaseConnector, Reshuffle } from 'reshuffle-base-connector'
import { Cipher } from './Cipher'

type MondayItem = Record<string, any>
type MondayBoardItems = Record<string, MondayItem>

const MONDAY_SYNC_TIMER_MS = parseInt(
  process.env.MONDAY_SYNC_TIMER_MS || '5000',
  10,
)

const UNDEFINED_NULL_PLACEHOLDER = 'UNDEFINED_OR_NULL'

interface Options {
  boardName: string
  monday: MondayConnector
  redis: RedisConnector
  encryptionKey?: string
  encryptedColumnList?: string[]
  apiUserIdsToIgnoreOnNewEvent?: string[]
}

export class MondayRedisService extends BaseConnector {
  public readonly boardName: string
  public readonly monday: MondayConnector
  public readonly redis: RedisConnector

  private boardId?: number
  private readonly keyBase: string
  private readonly namesKey: string
  private readonly changesKey: string
  private readonly cipher?: Cipher
  private encryptedColumns: Record<string, boolean> = {}
  private destageIntervalID: NodeJS.Timer | null = null
  private mondaySyncIntervalID: NodeJS.Timer | null = null

  constructor(app: Reshuffle, options: Options, id?: string) {
    super(app, options, id)

    this.boardName = MondayRedisService.validateBoardName(options.boardName)
    this.keyBase = `MondayBoard/${encodeURIComponent(this.boardName)}`
    this.namesKey = `${this.keyBase}/names`
    this.changesKey = `${this.keyBase}/changes`

    if (
      typeof options.monday !== 'object' ||
      options.monday.constructor.name !== 'MondayConnector'
    ) {
      throw new Error(`Not a monday connector: ${options.monday}`)
    }
    this.monday = options.monday

    if (
      typeof options.redis !== 'object' ||
      options.redis.constructor.name !== 'RedisConnector'
    ) {
      throw new Error(`Not a redis connector: ${options.redis}`)
    }
    this.redis = options.redis

    if (
      options.encryptionKey !== undefined &&
      options.encryptedColumnList !== undefined
    ) {
      if (!Array.isArray(options.encryptedColumnList)) {
        throw new Error(`Invalid column list: ${options.encryptedColumnList}`)
      }
      for (const title of options.encryptedColumnList) {
        if (typeof title !== 'string' || title.trim().length === 0) {
          throw new Error(`Invalid column list: ${options.encryptedColumnList}`)
        }
        this.encryptedColumns[title] = true
      }
      this.cipher = new Cipher(options.encryptionKey)
    }

    this.resetDestageInterval()
    this.resetMondaySync()
  }

  private resetDestageInterval() {
    if (this.destageIntervalID) {
      clearInterval(this.destageIntervalID)
    }

    this.destageIntervalID = setInterval(
      async () => await this.destageChanges(),
      MONDAY_SYNC_TIMER_MS,
    )
  }

  private resetMondaySync() {
    if (this.mondaySyncIntervalID) {
      clearInterval(this.mondaySyncIntervalID)
    }

    this.mondaySyncIntervalID = setInterval(
      async () => await this.syncBoardItemAdditionDeletion(),
      MONDAY_SYNC_TIMER_MS * 5,
    )
  }

  private getChangeKey(itemId: string, title: string) {
    return `${itemId}:${encodeURIComponent(title)}`
  }

  private async syncBoardItemAdditionDeletion() {
    if (this.boardId) {
      const mondayItemIds = await this.monday.getBoardItemIds(this.boardId)
      const redisItemIds = await this.getBoardItemIds()

      const createdItems = mondayItemIds.filter(
        (id) => !redisItemIds.includes(id),
      )
      const deletedItems = redisItemIds.filter(
        (id) => !mondayItemIds.includes(id),
      )

      for (const deletedItemId of deletedItems) {
        const existInMonday = mondayItemIds.includes(deletedItemId)
        if (!existInMonday) {
          this.app
            .getLogger()
            .info(
              `Item ${deletedItemId} removed from board ${this.boardName}, syncing Redis cache`,
            )
          await this.deleteItemInRedis(deletedItemId)
        }
      }

      for (const createdItemId of createdItems) {
        const exist = await this.redis.hexists(
          this.keyForItem(createdItemId),
          'id',
        )
        if (!exist) {
          this.app
            .getLogger()
            .info(
              `New item ${createdItemId} detected in board ${this.boardName}, syncing Redis cache`,
            )
          const res = await this.monday.getItem(parseInt(createdItemId, 10))
          const mondayItem =
            res && res.items && res.items.length && res.items[0]
          if (mondayItem) {
            await this.createItemInRedis({ id: createdItemId, ...mondayItem })
          }
        }
      }
    }
  }

  private async destageChanges(skipChangeKey?: string) {
    const changes = await this.redis.getset(this.changesKey, '')
    if (changes) {
      await Promise.all(
        (changes as string)
          .split(' ')
          .slice(1)
          .sort()
          .filter((e, i, a) => i === a.indexOf(e)) // unique
          .filter((e) => e !== skipChangeKey) // Skip skipChangeKey
          .map((change) => {
            const [itemId, title] = change.split(':')
            return this.destageOneChange(itemId, decodeURIComponent(title))
          }),
      )
    }
  }

  private async destageOneChange(itemId: string, title: string) {
    const serial = await this.redis.hget(this.keyForItem(itemId), title)
    const value = this.deserialize(serial, title)
    if (title === 'name') {
      await this.monday.updateItemName(await this.getBoardId(), itemId, value)
    } else {
      await this.monday.updateColumnValues(await this.getBoardId(), itemId, {
        [title]: () => value,
      })
    }
  }

  // Helpers ////////////////////////////////////////////////////////

  private keyForItem(itemId?: string) {
    // If the schema changes, you'll new to review the getBoardItemIds implementation
    return `${this.keyBase}/item/${itemId || '*'}`
  }

  private async getBoardItemIds(): Promise<string[]> {
    const keys = await this.redis.keys(this.keyForItem())
    return keys.map((key) => key.substr(key.lastIndexOf('/') + 1))
  }

  private serialize(value: any, title: string) {
    if (value === null || value === undefined) {
      return UNDEFINED_NULL_PLACEHOLDER
    }
    const json = JSON.stringify(value)
    return this.cipher && this.encryptedColumns[title]
      ? this.cipher.encrypt(json)
      : json
  }

  private deserialize(str: string | undefined, title: string) {
    if (!str || str === UNDEFINED_NULL_PLACEHOLDER) {
      return undefined
    }

    const json =
      this.cipher && this.encryptedColumns[title]
        ? this.cipher.decrypt(str)
        : str
    return JSON.parse(json)
  }

  private async getMondayBoardItems(): Promise<MondayBoardItems> {
    let board = await this.monday.getBoardItems(await this.getBoardId())

    if (!board || board.name !== this.boardName) {
      this.boardId = undefined
      board = await this.monday.getBoardItems(await this.getBoardId())
    }

    if (!board || board.name !== this.boardName) {
      throw new Error(`Unable to read Monday board: ${this.boardName}`)
    }

    return board.items
  }

  private async createItemInRedis(item: MondayItem): Promise<void> {
    await Promise.all([
      ...Object.keys(item).map((title) =>
        this.redis.hset(
          this.keyForItem(item.id),
          title,
          this.serialize(item[title], title),
        ),
      ),
      this.redis.hset(this.namesKey, item.name, item.id),
    ])
  }

  private async deleteItemInRedis(itemId: string): Promise<void> {
    const item = await this.getBoardItemById(itemId)
    if (item) {
      await Promise.all([
        this.redis.del(this.keyForItem(item.id)),
        this.redis.hdel(this.namesKey, item.name),
      ])
    }
  }

  private async updateColumnValue(
    itemId: string,
    title: string,
    update: Promise<any>,
  ) {
    const change = this.getChangeKey(itemId, title)
    return Promise.all([
      update,
      this.redis.append(this.changesKey, ' ' + change),
    ])
  }

  // Initialize the Redis mirror for this board.
  public async initialize(): Promise<MondayRedisService> {
    this.app.getLogger().info(`Initializing ${this.boardName} board`)
    const initialized = await this.redis.setnx(this.changesKey, '')
    if (initialized === 1) {
      this.app.getLogger().info(`Populating Redis mirror for ${this.boardName}`)
      const items = await this.getMondayBoardItems()
      await Promise.all(
        Object.values(items).map((item) => this.createItemInRedis(item)),
      )
    }

    this.boardId = await this.getBoardId()
    this.monday.on(
      {
        type: 'ChangeColumnValue',
        boardId: this.boardId,
      },
      async (event) => {
        const { boardId, itemId, columnTitle, columnType, value, userId } = event

        if (this.configOptions.apiUserIdsToIgnoreOnNewEvent && this.configOptions.apiUserIdsToIgnoreOnNewEvent.includes(userId)) {
          this.app
              .getLogger()
              .info(
                  `Monday event received - skip changes from user: ${userId} (listed in configOptions.apiUserIdsToIgnoreOnNewEvent)`)
          return
        }

        let newValue
        switch (columnType) {
          case 'location':
            const res = await this.monday.getItem(parseInt(itemId, 10))
            const mondayItem =
              res && res.items && res.items.length && res.items[0]
            newValue = mondayItem?.Location
            break
          case 'color':
            newValue = value.label
            break
          case 'board-relation':
            newValue = { linkedPulseIds: value.linkedPulseIds }
            break
          case 'text':
          case 'numeric':
            newValue = value ? value.value : value
            break
          case 'dropdown':
            newValue = value.chosenValues
            break
          default:
            newValue = value
        }

        if (this.boardId === parseInt(boardId, 10)) {
          this.app
            .getLogger()
            .info(
              `Monday event received - Update Redis cache with ${JSON.stringify(
                newValue,
              )} for itemId ${itemId} (title: ${columnTitle})`,
            )
          await this.redis.hset(
            this.keyForItem(itemId),
            columnTitle,
            this.serialize(newValue, columnTitle),
          )

          this.resetMondaySync()
          await this.destageChanges(this.getChangeKey(itemId, columnTitle))
        }
      },
    )

    this.monday.on(
      {
        type: 'CreateItem',
        boardId: this.boardId,
      },
      async (event) => {
        const { boardId, itemId, itemName, userId } = event

        if (this.configOptions.apiUserIdsToIgnoreOnNewEvent && this.configOptions.apiUserIdsToIgnoreOnNewEvent.includes(userId)) {
          this.app
              .getLogger()
              .info(
                  `Monday event received - skip changes from user: ${userId} (listed in configOptions.apiUserIdsToIgnoreOnNewEvent)`)
          return
        }

        if (this.boardId === parseInt(boardId, 10)) {
          this.app
            .getLogger()
            .info(
              `Monday event received - Create new item in Redis cache (itemId: ${itemId}, itemName: ${itemName})`,
            )
          const response = await this.monday.getItem(parseInt(itemId, 10))
          if (response && response.items && response.items[0]) {
            this.resetMondaySync()
            await this.createItemInRedis({ id: itemId, ...response.items[0] })
          }
        }
      },
    )

    return this
  }

  // Create a new item in Monday and the Redis mirror.
  //
  // @param name item name
  // @param columnValues values by column titles
  //
  // @return item object
  //
  public async createItem(
    name: string,
    columnValues: Record<string, any>,
  ): Promise<MondayItem> {
    const boardId = await this.getBoardId()
    const columnUpdaters: Record<string, () => any> = {}
    for (const [title, value] of Object.entries(columnValues)) {
      columnUpdaters[title] = () => value
    }
    const itemId = await this.monday.createItem(boardId, name, columnUpdaters)
    const item = { id: itemId, name, ...columnValues }
    await this.createItemInRedis(item)
    return item
  }

  // Get the id of this board.
  //
  // @return board id
  //
  public async getBoardId(): Promise<number> {
    if (!this.boardId) {
      this.boardId = await this.monday.getBoardIdByName(this.boardName)
      if (!this.boardId) {
        throw new Error(`Monday board not found: ${this.boardName}`)
      }
    }
    return this.boardId
  }

  // Get all items.
  //
  //
  // @return an array of objects with the item's id, name and column
  //         value (undefined if not found)
  //
  public async getBoardItems(): Promise<MondayItem[]> {
    const ids = await this.getBoardItemIds()

    const items: MondayItem[] = []
    if (ids) {
      for (const id of ids) {
        const item = await this.getBoardItemById(id)

        item && items.push(item)
      }
    }

    return items
  }

  // Get an item with the specified id from this board.
  //
  // @param itemId item id
  //
  // @return an object with the item's id, name and column
  //         value (undefined if not found)
  //
  public async getBoardItemById(
    itemId: string,
  ): Promise<MondayItem | undefined> {
    MondayRedisService.validateId(itemId)

    const raw = await this.redis.hgetall(this.keyForItem(itemId))
    if (raw) {
      const item: any = {}
      for (const title of Object.keys(raw)) {
        item[title] = this.deserialize(raw[title], title)
      }
      return item
    }
  }

  // Get an item with the specified name from this board.
  //
  // @param name item name
  //
  // @return an object with the item's id, name and column
  //         value (undefined if not found)
  //
  public async getBoardItemByName(
    name: string,
  ): Promise<MondayItem | undefined> {
    MondayRedisService.validateBoardName(name)
    const id = await this.redis.hget(this.namesKey, name)
    return id && this.getBoardItemById(id)
  }

  // Set a new value for one column of a specific item.
  //
  // The new value is atomically written to the Redis mirror, and will
  // eventually be synchronized back to Monday. This assures updates
  // are atomic and reduces load on Monday, as multiple updates to the
  // same column are consolidated before Monday is updated.
  //
  // The new value is encrypted if necessary before caching.
  //
  // @param itemId id of the Monday item to be updated
  // @param title title of the Monday column to be updated
  // @param value new value
  //
  public async setColumnValue(
    itemId: string,
    title: string,
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    value: any,
  ): Promise<void> {
    await this.updateColumnValue(
      itemId,
      title,
      this.redis.hset(
        this.keyForItem(itemId),
        title,
        this.serialize(value, title),
      ),
    )
  }

  public async setItemName(itemId, name: string): Promise<void> {
    const item = await this.getBoardItemById(itemId)
    await this.setColumnValue(itemId, 'name', name)
    if (item) {
      await this.redis.hdel(this.namesKey, item.name)
    }
    await this.redis.hset(this.namesKey, name, itemId)
  }
  // Increase (or decrease) the value for one column of a specific
  // item. The column must have a numeric value and must not be
  // encrypted.
  //
  // The new value is atomically written to the Redis mirror, and will
  // eventually be synchronized back to Monday. This assures updates
  // are atomic and reduces load on Monday, as multiple updates to the
  // same column are consolidated before Monday is updated.
  //
  // @param itemId id of the Monday item to be updated
  // @param title title of the Monday column to be updated
  // @param incr numeric increment (negative to decrement)
  //
  public async incrColumnValue(
    itemId: string,
    title: string,
    incr = 1,
  ): Promise<void> {
    await this.updateColumnValue(
      itemId,
      title,
      this.redis.hincrby(this.keyForItem(itemId), title, incr),
    )
  }

  // Static /////////////////////////////////////////////////////////

  // Validate an item id and throw an error otherwise.
  //
  // @param itemId item id
  //
  // @return same item id
  //
  public static validateId(itemId: string): string {
    if (typeof itemId !== 'string' || !/^\d{9,10}$/.test(itemId)) {
      throw new Error(`Invalid item id: ${itemId}`)
    }
    return itemId
  }

  // Validate an item name and throw an error otherwise.
  //
  // @param name item name
  //
  // @return same item name
  //
  public static validateBoardName(name: string): string {
    if (typeof name !== 'string' || name.trim().length === 0) {
      throw new Error(`Invalid name: ${name}`)
    }
    return name
  }
}

// Create a new service.
//
// @param boardName name of board
// @param monday pre-configured Reshuffle Monday connector
// @param redis pre-configured Reshuffle Redis connector
// @param encryptionKey optional key for encrypting data in Redis (if
//                      not specified data is mirrored in the clear)
// @param encryptedColumnList optional list of columns to be encrypted
//
// @return new board accessor
//
export async function createAndInitializeMondayRedisService(
  app: Reshuffle,
  boardName: string,
  monday: MondayConnector,
  redis: RedisConnector,
  encryptionKey?: string,
  encryptedColumnList?: string[],
  apiUserIdsToIgnoreOnNewEvent?: string[],
): Promise<MondayRedisService> {
  const mrs = new MondayRedisService(app, {
    boardName,
    monday,
    redis,
    encryptionKey,
    encryptedColumnList,
    apiUserIdsToIgnoreOnNewEvent,
  })
  return mrs.initialize()
}
