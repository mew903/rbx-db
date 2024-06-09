-- RbxDb
-- mew903, 2024
assert(game:GetService('RunService'):IsServer(), 'RbxDb can only be used in Server Scripts')

-- TODO: rollbacks using memory store service

-- debug flags
local __DEBUG__ = true -- turns all debug flags on
local __PING__ = false -- ping when a new connection is made
local __VERBOSE__ = false -- print debug info
local __WARNINGS__ = true -- warn for misuse / code smell

-- usage flags
local DEFAULT_SCOPE = 'global' -- roblox default
local MAX_REQUEST_RETRIES = 10

-- types
type map<K, V> = { [K]: V }
type array<T> = map<number, T>

type weakmap<K, V> = typeof(setmetatable({ } :: map<K, V>, { } :: {
  __metatable: boolean,
  __mode: string,
}))

export type key = number | string
export type data = number | string | map<key, data>
export type version = number | string

-- callbacks
type callback<A..., R...> = (A...) -> R...
type async<A..., R...> = callback<(RbxDb, key, A...), R...>
type result<V, A...> = callback<(V, A...)>

-- interfaces
type RbxDbImpl = {
  -- async class methods
  DeleteAsync: (self: RbxDb, key: key) -> data?,
  FetchAsync: (self: RbxDb, key: key, retry: boolean?, opt: DataStoreGetOptions?) -> data?,
  PingAsync: (self: RbxDb) -> (boolean, number),
  RollbackAsync: (self: RbxDb, key: key, version: version) -> data?,
  SetAsync: (self: RbxDb, key: key, value: data, uids: array<number>?, opt: DataStoreSetOptions?) -> string,
  UpdateAsync: (self: RbxDb, key: key, transform: (data, DataStoreKeyInfo) -> data) -> (data, DataStoreKeyInfo),
  -- class methods
  Delete: (self: RbxDb, key: key) -> (value: result<data?>) -> (),
  Fetch: (self: RbxDb, key: key, retry: boolean?, opt: DataStoreGetOptions?) -> (callback: result<data?>) -> (),
  Ping: (self: RbxDb) -> (callback: callback<(boolean, number)>) -> (),
  Rollback: (self: RbxDb, key: key) -> (version: version) -> result<data?>,
  Set: (self: RbxDb, key: key, uids: array<number>?, opt: DataStoreSetOptions?) -> (value: data, callback: result<version?>?) -> (),
  Update: (self: RbxDb, key: key) -> (transform: (data, DataStoreKeyInfo) -> data, callback: result<data?, DataStoreKeyInfo>?) -> (),
  -- metamethods
  __index: RbxDbImpl,
  __metatable: boolean,
  __tostring: (self: RbxDb) -> string,
  -- static methods
  connect: (name: string, scope: string?, ordered: boolean?, opt: DataStoreOptions?) -> RbxDb,
}

-- "classes"
export type RbxDb = typeof(setmetatable({ } :: {
  datastore: DataStore,
  keys: weakmap<string, number>,
  locked: weakmap<string, boolean>,
  name: string,
  ordered: boolean,
  scope: string,
  print: <A...>(A...) -> (),
  warn: <A...>(A...) -> (),
}, { } :: RbxDbImpl))

-- constants
-- you can remove these
local assert = assert
local print = print
local select = select
local setmetatable = setmetatable
local tick = tick
local tostring = tostring
local xpcall = xpcall
local warn = warn
-- you can replace these
local clock = os.clock
local empty = { }
local floor = math.floor
local pack = table.pack
local resume = coroutine.resume
local thread = coroutine.create
local wait = task.wait
-- error messages
local ORDERED_INT_ONLY = "can only store postive integers in OrderedDataStore"

-- code
local Players = game:GetService('Players') 
local DataStoreService = game:GetService('DataStoreService')

-- TODO: cache wrapper, rollbacks
-- local MemoryStoreService = game:GetService('MemoryStoreService')

if __DEBUG__ then
  __PING__ = true
  __VERBOSE__ = true
  __WARNINGS__ = true
end

local function datastore(name: string, scope: string, ordered: boolean?, opt: DataStoreOptions?): DataStore
  if ordered then
    return DataStoreService:GetOrderedDataStore(name, scope)
  end
  return DataStoreService:GetDataStore(name, scope, opt)
end

local function deleteAsync(source: RbxDb, key: key): data?
  return source.datastore:RemoveAsync(tostring(key))
end

local function isInt(n: unknown): boolean
  return type(n) == 'number' and floor(n) == n
end

local function request<A..., R...>(f: async<A..., R...>, source: RbxDb, key: key, retry: boolean, ...: A...): R...
  local reponse
  local tries = 0
  repeat
    tries += 1
    wait(tries * (60 / (60 + (#Players:GetPlayers() * 10))))
    reponse = { select(2, xpcall(f, source.warn, source, key, ...)) }
  until not retry or tries == MAX_REQUEST_RETRIES or #reponse > 0
  if tries == MAX_REQUEST_RETRIES then
    -- TODO: use memory service for request fallback queue
  end
  return unpack(reponse)
end

local function round(n: number, d: number): number
  return floor(n * 10 ^ d) / 10 ^ d
end

local function fetchAsync(source: RbxDb, key: key, opt: DataStoreGetOptions?): data?
  return source.datastore:GetAsync(tostring(key), opt)
end

local function setAsync(source: RbxDb, key: key, value: data, uids: array<number>?, opt: DataStoreSetOptions?): string
  if source.ordered then
    return source.datastore:SetAsync(tostring(key), value, nil, opt)
  end
  return source.datastore:SetAsync(tostring(key), value, uids or empty, opt)
end

local function updateAsync(source: RbxDb, key: key, value: result<data, DataStoreKeyInfo>): (data, DataStoreKeyInfo)
  return source.datastore:UpdateAsync(tostring(key), value)
end

local RbxDb: RbxDbImpl = { } :: RbxDbImpl do
  function RbxDb.DeleteAsync(self, key)
    return request(deleteAsync, self, key, true)
  end

  function RbxDb.FetchAsync(self, key, retry, opt)
    return request(fetchAsync, self, key, retry or false, opt)
  end

  function RbxDb.PingAsync(self)
    local utime = clock()
    local alive = xpcall(setAsync, self.warn, self, '__ping__', floor(utime))
    local well = xpcall(fetchAsync, self.warn, self, '__ping__')
    return alive and well, round(clock() - utime, 3) -- should be less than 1s if alls good
  end

  function RbxDb.RollbackAsync(self, key, version)
    error(`[RbxDb][{ self }] RbxDb::Rollback not yet implemented`)
  end

  function RbxDb.SetAsync(self, key, value, uids, opt)
    assert(not self.ordered or isInt(value), `arg #1 invalid: { ORDERED_INT_ONLY }`)
    return request(setAsync, self, key, true, value, uids, opt)
  end

  function RbxDb.UpdateAsync(self, key, transform)
    return request(updateAsync, self, key, true, function(value, info)
      assert(not self.ordered or isInt(value), `arg #1 invalid: { ORDERED_INT_ONLY }`)
      transform(value, info)
    end)
  end

  function RbxDb.Delete(self, key)
    return function(callback)
      resume(thread(function()
        callback(self:DeleteAsync(key))
      end))
    end
  end

  function RbxDb.Fetch(self, key, retry, opt)
    return function(callback)
      resume(thread(function()
        callback(self:FetchAsync(key, retry, opt))
      end))
    end
  end

  function RbxDb.Ping(self)
    return function(callback)
      resume(thread(function()
        callback(self:PingAsync())
      end))
    end
  end

  function RbxDb.Rollback(self, key)
    error(`[RbxDb][{ self }] RbxDb::Rollback not yet implemented`)
  end

  function RbxDb.Set(self, key, uids, opt)
    return function(value, callback)
      resume(thread(function()
        if type(callback) == 'function' then
          if self.ordered then
            self.warn('OrderedDataStore does not support versioning, passing null to callback')
          end
          callback(self:SetAsync(key, value, uids, opt))
          return
        end
        self:SetAsync(key, value, uids, opt)
      end))
    end
  end

  function RbxDb.Update(self, key)
    return function(transform, callback)
      resume(thread(function()
        if type(callback) == 'function' then
          callback(self:UpdateAsync(key, transform))
          return
        end
        self:UpdateAsync(key, transform)
      end))
    end
  end

  RbxDb.__index = RbxDb
  RbxDb.__metatable = true
  RbxDb.__tostring = function(self)
    return `{ self.name }@{ self.scope }{ self.ordered and '.ordered' or '' }`
  end

  -- use existing connections if possible, e.g. to prevent unnecessary pings
  -- but if we're connecting infrequently or on-demand then let luau gc do its thing
  local schema = setmetatable({ }, {
    __index = function(self, k)
      return setmetatable({ }, {
        __metatable = true,
        __mode = 'kv',
      })
    end,
    __metatable = true,
    __mode = 'kv',
  })

  function RbxDb.connect(name, scope, ordered, opt)
    assert(not ordered or not opt, 'arg #4 invalid: cannot pass DataStoreOptions to OrderedDataStore')
    local scope = scope or DEFAULT_SCOPE
    local connection = schema[name][scope]
    if connection then
      return connection
    end
    connection = setmetatable({
      datastore = datastore(name, scope, ordered, opt),
      keys = setmetatable({ }, {
        __metatable = true,
        __mode = 'kv',
      }),
      locked = setmetatable({ }, {
        __metatable = true,
        __mode = 'kv',
      }),
      name = name,
      ordered = ordered or false,
      scope = scope,
      print = function<A...>(...: A...)
        print(`[RbxDb][{ connection }]`, ...)
      end,
      warn = function<A...>(...: A...)
        warn(`[RbxDb][{ connection }]`, ...)
      end,
    }, RbxDb)
    schema[name][scope] = connection
    if __PING__ then
      local result, elapsed = connection:PingAsync()
      if result then
        connection.print(`connected (elapsed: { elapsed }s)`)
      else
        connection.warn(`connection failed (elapsed: { elapsed }s)`)
      end
    end
    return connection
  end
end

return RbxDb
