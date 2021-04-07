import { MondayConnector } from 'reshuffle-monday-connector'

type MondayItem = Record<string, any>
type MondayGroupIdTitle = [string, string]

export { MondayItem, MondayGroupIdTitle }

export const GROUP_ID_COLUMN = '_groupId'

const GET_ITEM_QUERY = `#graphql
query ($itemId: [Int]!) {
  items (ids: $itemId) {
    id
    name
    column_values {
      title
      type
      value
    }
    group {
      id
    }
  }
}`

const GET_BOARD_ITEMS_QUERY = `#graphql
query ($boardId: [Int]!) {
  boards (ids: $boardId) {
    id
    name
    items {
      id
      name
      column_values {
        title
        type
        value
      }
      group {
        id
      }
    }
  }
}`

const GET_BOARD_GROUPS_QUERY = `#graphql
query ($boardId: [Int]!) {
  boards (ids: $boardId) {
    id
    name
    groups {
      id
      title
    }
  }
}`

const CREATE_ITEM_QUERY = `#graphql
mutation ($boardId: Int!, $groupId: String, $itemName: String, $columnValues: JSON) {
  create_item (board_id: $boardId, group_id: $groupId, item_name: $itemName, column_values: $columnValues) {
    id
    group {
      id
    }
  }
}`

const UPDATE_GROUP_ID_QUERY = `#graphql
    mutation ($itemId: Int!, $groupId: String!) {
      move_item_to_group (item_id: $itemId, group_id: $groupId) {
        id
      }
  }`

function itemDataToObject(
  item: any,
  mapper: (value: any) => any = (x) => x,
): Record<string, any> {
  const obj: Record<string, any> = {}
  for (const cv of item.column_values) {
    const str = JSON.parse(cv.value)
    const val = cv.type === 'numeric' ? parseInt(str) : str
    obj[cv.title] = mapper(val)
  }
  obj.name = item.name
  obj.id = item.id
  obj[GROUP_ID_COLUMN] = item.group.id
  return obj
}

function columnValueUpdatersToObject(
  columnValues: Record<string, (value?: any) => any>,
  boardColumns: Record<string, any>,
): Record<string, any> {
  const newValues: any = {}
  for (const [key, updater] of Object.entries(columnValues)) {
    const bc = boardColumns.find(({ title }) => title === key)
    if (!bc) {
      throw new Error(`Column title not found: ${key}`)
    }

    if (typeof updater !== 'function') {
      throw new Error(`Missing or invalid updater for: ${key}`)
    }
    const newValue = updater()
    newValues[bc.id] =
      typeof newValue !== 'object' ? String(newValue) : newValue
  }

  return newValues
}

export class MondayHelper {
  public readonly monday: MondayConnector

  constructor(monday: MondayConnector) {
    this.monday = monday
  }

  async getBoardItem(itemId: number): Promise<Record<string, any> | undefined> {
    const res = await this.monday.query(GET_ITEM_QUERY, { itemId: itemId })
    if (!res || !res.items || !res.items.length) {
      return undefined
    }
    return itemDataToObject(res.items[0])
  }

  async getBoardItems(
    boardId: number,
  ): Promise<{
    name: string
    items: MondayItem[]
  }> {
    const res = await this.monday.query(GET_BOARD_ITEMS_QUERY, {
      boardId: boardId,
    })
    const board = res?.boards[0]
    const items: MondayItem[] = []
    for (const item of board.items) {
      items.push(itemDataToObject(item))
    }
    return { name: board.name, items }
  }

  async getBoardGroups(boardId: number): Promise<MondayGroupIdTitle[]> {
    const res = await this.monday.query(GET_BOARD_GROUPS_QUERY, {
      boardId: boardId,
    })
    const board = res?.boards[0]
    const groups: MondayGroupIdTitle[] = []
    for (const g of board.groups) {
      groups.push([g.id, g.title])
    }
    return groups
  }

  async updateItemGroup(itemId: number, groupId: string): Promise<void> {
    await this.monday.query(UPDATE_GROUP_ID_QUERY, {
      itemId: itemId,
      groupId: groupId,
    })
  }

  async createItem(
    boardId: number,
    itemName?: string,
    columnValues?: Record<string, (value: string) => any>,
    groupId?: string,
  ): Promise<[string, string]> {
    const boardColumnsResp = await this.monday.getColumn(boardId)
    const boardColumns = boardColumnsResp?.boards[0].columns

    const res = await this.monday.query(CREATE_ITEM_QUERY, {
      boardId: boardId,
      groupId: groupId,
      itemName: itemName,
      columnValues:
        columnValues && boardColumns
          ? JSON.stringify(
              columnValueUpdatersToObject(columnValues, boardColumns),
            )
          : undefined,
    })
    const create_item = (res as any).create_item

    return [create_item.id, create_item.group.id]
  }
}
