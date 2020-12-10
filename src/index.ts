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

interface Options {
  boardName: string
  monday: MondayConnector
  redis: RedisConnector
  encryptionKey?: string
  encryptedColumnList?: string[]
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

    setInterval(() => this.destageChanges(), MONDAY_SYNC_TIMER_MS)
  }

  private getChangeKey(itemId: string, title: string) {
    return ` ${itemId}:${encodeURIComponent(title)}`
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
          .filter((v) => v !== skipChangeKey) // Don't process if changeKey equals skipChangeKey
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
    await this.monday.updateColumnValues(await this.getBoardId(), itemId, {
      [title]: () => value,
    })
  }

  // Helpers ////////////////////////////////////////////////////////

  private keyForItem(itemId: string) {
    return `${this.keyBase}/item/${itemId}`
  }

  private serialize(value: any, title: string) {
    const json = JSON.stringify(value)
    const secret = this.cipher && this.encryptedColumns[title]
    return secret ? this.cipher!.encrypt(json) : json
  }

  private deserialize(str: string, title: string) {
    const secret = this.cipher && this.encryptedColumns[title]
    const json = secret ? this.cipher!.decrypt(str) : str
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

  private async updateColumnValue(
    itemId: string,
    title: string,
    update: Promise<any>,
  ) {
    const change = this.getChangeKey(itemId, title)
    return Promise.all([update, this.redis.append(this.changesKey, change)])
  }

  // Actions ////////////////////////////////////////////////////////

  // Initialize the Redis mirror for this board.
  public async initialize() {
    this.app.getLogger().info(`Initializing ${this.boardName} board`)
    const initialized = await this.redis.setnx(this.changesKey, '')
    if (initialized === 1) {
      this.app.getLogger().info(`Populating Redis mirror for ${this.boardName}`)
      const items = await this.getMondayBoardItems()
      await Promise.all(
        Object.values(items)
          .map((item) => [
            ...Object.keys(item).map((title) =>
              this.redis.hset(
                this.keyForItem(item.id),
                title,
                this.serialize(item[title], title),
              ),
            ),
            this.redis.hset(this.namesKey, item.name, item.id),
          ])
          .flat(),
      )
    }

    this.boardId = await this.getBoardId()
    this.monday.on(
      {
        type: 'ChangeColumnValue',
        boardId: this.boardId,
      },
      async (event) => {
        const {
          boardId,
          itemId,
          columnTitle,
          value: { value },
        } = event
        if (this.boardId === parseInt(boardId, 10)) {
          this.app
            .getLogger()
            .info(
              `Monday event received - Update Redis cache with ${value} for itemId ${itemId} (title: ${columnTitle})`,
            )
          await this.redis.hset(
            this.keyForItem(itemId),
            columnTitle,
            this.serialize(value, columnTitle),
          )
          await this.destageChanges(
            this.getChangeKey(itemId, columnTitle).trim(),
          )
        }
      },
    )

    return this
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

  // Get an item with the specified id from this board.
  //
  // @param itemId item id
  //
  // @return an object with the item's id, name and column
  //         value (undefined if not found)
  //
  public async getBoardItemById(itemId: string) {
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
  public async getBoardItemByName(name: string) {
    MondayRedisService.validateBoardName(name)
    const id = await this.redis.hget(this.namesKey, name)
    if (!id) {
      throw new Error(`Item not found: ${name}`)
    }
    return this.getBoardItemById(id)
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
  public async setColumnValue(itemId: string, title: string, value: any) {
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
  public async incrColumnValue(itemId: string, title: string, incr = 1) {
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
  public static validateBoardName(name: string) {
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
) {
  const mrs = new MondayRedisService(app, {
    boardName,
    monday,
    redis,
    encryptionKey,
    encryptedColumnList,
  })
  return mrs.initialize()
}
