import { Context, Database, Dict, Loader, makeArray, Schema, Tables } from 'koishi'
import { Notifier } from '@koishijs/plugin-notifier'

class Migrater {
  ctx: Context
  dbFrom: Database
  dbTo: Database
  notifier: Notifier
  status: Dict = Object.create(null)
  _updateStatus: () => void

  constructor(ctx: Context, private config: Migrater.Config) {
    this.notifier = ctx.notifier.create()

    ctx.on('ready', async () => {
      try {
        this.ctx = this.setup(ctx)
      } catch {
        this.notifier.update('目标数据库插件未找到')
        return
      }

      this._updateStatus = ctx.debounce(() => {
        const status = Object.values(this.status).map(x => <p>{x}</p>)
        this.notifier.update(status)
      }, 500)

      this.ctx.inject(['database'], async () => {
        this.dbFrom = ctx.database
        this.dbTo = this.ctx.database
        this.ctx.model.tables = ctx.model.tables
        this.notifier.update(<>
          <p>当前使用的数据库: {this.dbFrom.drivers.default.constructor.name}</p>
          <p>迁移的目标数据库: {this.dbTo.drivers.default.constructor.name}</p>
          <p><button onClick={this.run.bind(this)}>开始迁移</button></p>
        </>)
      })
    })
  }

  setup(_ctx: Context) {
    const key = Object.keys(_ctx.scope.parent.config).find(key => key.includes('database-'))
    const config = _ctx.scope.parent.config[key]
    const ctx = _ctx.isolate('model').isolate('database')
    ctx.scope[Loader.kRecord] = ctx.root.scope[Loader.kRecord]
    ctx.plugin(Database)
    ctx.loader.reload(ctx, key.slice(1, key.lastIndexOf(':')), config)
    return ctx
  }

  async migrateTable(table: keyof Tables, options: {
    batchsize: number
    count: number
  }) {
    const model = this.dbFrom.tables[table]
    for (let i = 0; i < options.count; i += options.batchsize) {
      this.updateStatus(table, `- ${table} (${i}/${options.count})`)
      let sel = this.dbFrom.select(table)
      makeArray(model.primary).forEach(key => sel = sel.orderBy(key))
      const batch = await sel.limit(options.batchsize).offset(i).execute()
      await this.dbTo.upsert(table, batch)
    }
    this.updateStatus(table, `√ ${table} (${options.count})`)
  }

  updateStatus(table, content) {
    this.status[table] = content
    this._updateStatus()
  }

  async run() {
    const stats = await this.dbFrom.stats()
    await Promise.all(Object.keys(this.dbFrom.tables).map(async (table: keyof Tables) => {
      await this.migrateTable(table, {
        batchsize: this.config.batchsize,
        count: stats.tables[table].count,
      })
    }))
    this.updateStatus('', '迁移完成')
  }
}

namespace Migrater {
  export const usage = '将本插件与要迁移的目标数据库插件放在同一分组内。启用本插件即开始迁移。'

  export const inject = ['database', 'notifier']

  export interface Config {
    batchsize: number
  }

  export const Config: Schema<Config> = Schema.object({
    batchsize: Schema.number().default(1000),
  })
}

export default Migrater
