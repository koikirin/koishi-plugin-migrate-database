import { Context, Database, Dict, Driver, Loader, makeArray, mapValues, Schema, Tables } from 'koishi'
import { } from '@koishijs/plugin-notifier'

class Migrater {
  static name = 'migrate-database'

  ctx: Context
  dbFrom: Database<Tables>
  dbTo: Database<Tables>

  _status: Dict<string> = Object.create(null)
  _filters: Dict<boolean> = Object.create(null)
  _updateStatus: () => void

  constructor(ctx: Context, private config: Migrater.Config) {
    const notifier = ctx.notifier.create()

    ctx.on('ready', async () => {
      try {
        this.ctx = this.setup(ctx)
      } catch {
        notifier.update('目标数据库插件未找到')
        return
      }

      this._updateStatus = ctx.throttle(() => {
        const status = Object.values(this._status).map(x => <p>{x}</p>)
        notifier.update(status)
      }, 400)

      this.ctx.inject(['database'], async () => {
        this.dbFrom = ctx.database
        this.dbTo = this.ctx.database
        this.ctx.model.tables = mapValues(ctx.model.tables, (model) => {
          const model2 = Object.create(model)
          model2.ctx = this.ctx
          return model2
        })
        this._filters = Object.fromEntries(Object.keys(this.dbFrom.tables).map(key => [key, true]))
        this.ctx.database.refresh()
        await this.ctx.database.prepared()

        const stats = await this.dbFrom.stats()
        const switchFilter = (key: string) => {
          this._filters[key] = !this._filters[key]
          notify()
        }
        const notify = () => notifier.update(<>
          <p>当前使用的数据库: {Object.values(this.dbFrom.drivers)[0].constructor.name}</p>
          <p>迁移的目标数据库: {Object.values(this.dbTo.drivers)[0].constructor.name}</p>
          {Object.entries(this._filters).map(([key, value]) => (
            <p><button type={value ? 'success' : 'default'} onClick={() => switchFilter(key)}>
              {value ? '已选中' : '未选中'}
            </button> {key} ({stats.tables[key].count})</p>
          ))}
          <p><button type="primary" onClick={this.run.bind(this, stats)}>开始迁移</button></p>
        </>)

        notify()
      })
    })
  }

  setup(_ctx: Context) {
    class Database2 extends Database { }

    const key = Object.keys(_ctx.scope.parent.config).find(key => key.includes('database-'))
    const config = _ctx.scope.parent.config[key]
    const ctx = _ctx.isolate('model').isolate('database')
    ctx.scope[Loader.kRecord] = ctx.root.scope[Loader.kRecord]
    ctx.plugin(Database2)
    ctx.loader.reload(ctx, key.slice(1, key.lastIndexOf(':')), config)
    return ctx
  }

  async migrateTable(table: keyof Tables, options: {
    batchsize: number
    count: number
  }) {
    const model = this.dbFrom.tables[table]
    for (let i = 0; i < options.count; i += options.batchsize) {
      this.updateStatus(table, `- ${table} (${i} / ${options.count})`)
      let sel = this.dbFrom.select(table)
      makeArray(model.primary).forEach(key => sel = sel.orderBy(key as any))
      const batch = await sel.limit(options.batchsize).offset(i).execute()
      await this.dbTo.upsert(table, batch)
    }
    this.updateStatus(table, `✅ ${table} (${options.count})`)
  }

  updateStatus(table: string, content: string) {
    this._status[table] = content
    this._updateStatus()
  }

  async run(stats: Driver.Stats) {
    await Promise.all(Object.entries(this._filters).map(async ([table, filter]: [keyof Tables, boolean]) => {
      if (!filter) return
      try {
        await this.migrateTable(table, {
          batchsize: this.config.batchsize,
          count: stats.tables[table].count,
        })
      } catch (e) {
        this.ctx.logger.warn(e)
        this.updateStatus(table, `❌ ${table} (Error: ${e})`)
      }
    }))
    this.updateStatus('', '迁移完成，请关闭此插件')
  }
}

namespace Migrater {
  export const usage = '将本插件与要迁移的目标数据库插件放在同一分组内。启用本插件跟随指引开始迁移。'
  export const filter = false
  export const inject = ['database', 'notifier']

  export interface Config {
    batchsize: number
  }

  export const Config: Schema<Config> = Schema.object({
    batchsize: Schema.natural().default(1000),
  })
}

export default Migrater
