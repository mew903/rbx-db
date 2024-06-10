-- RbxDb
-- mew903, 2021 - 2024
assert(game:GetService('RunService'):IsServer(), 'RbxDb can only be used in Server Scripts')

-- TODO: rollbacks using memory store service

-- debug flags
local __DEBUG__ = true -- turns all debug flags on
local __PING__ = false -- ping when a new connection is made
local __VERBOSE__ = false -- print debug info & warnings
local __WARNINGS__ = true -- throws warnings for misuse / code smell

-- usage flags
local DEFAULT_SCOPE = 'global' -- roblox default

-- seconds to wait between requests for the same key
local KEY_REQUEST_THROTTLE = 6

-- max # of retries before fallback actions take place
local MAX_REQUEST_RETRIES = 10

-- time format string for prints and warns
-- non-americans will likely want: TIME_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
local TIME_FORMAT = 'YYYY-MM-DD HH12:MI:SS XM'

-- types
type map<K, V> = { [K]: V }
type array<T> = map<number, T>

type weakmap<K, V> = typeof(setmetatable({ } :: map<K, V>, { } :: {
  __metatable: boolean,
  __mode: string,
}))

export type key = string
export type data = boolean | number | string | map<key | number, data>
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
  keys: weakmap<key, number>,
  locked: weakmap<key, boolean>,
  name: string,
  ordered: boolean,
  scope: string,
  print: <A...>(unknown, A...) -> (),
  wait: (key) -> number,
  warn: <A...>(unknown, A...) -> (),
}, { } :: RbxDbImpl))

-- you can remove these
local assert = assert
local print = print
local select = select
local setmetatable = setmetatable
local tick = tick
local xpcall = xpcall
local warn = warn

-- you can replace these
local clock = os.clock
local find = table.find
local floor = math.floor
local gsub = string.gsub
local pack = table.pack
local remove = table.remove
local resume = coroutine.resume
local thread = coroutine.create
local wait = task.wait

-- error messages
local E_ORDERED_INT_ONLY = 'Can only store postive integers in OrderedDataStore'
-- warning messages
local W_ORDERED_SET_VERS = 'OrderedDataStore does not support versioning, passing null to callback'
-- constants
local EMPTY = { }
local NULL_VALUE = '__RBXDB_NULL__'
-- volatiles
local requests = 0

-- code
local Players = game:GetService('Players') 
local DataStoreService = game:GetService('DataStoreService')

-- TODO: cache wrapper, rollbacks
-- local MemoryStoreService = game:GetService('MemoryStoreService')

local function isInt(n: unknown): boolean
  return type(n) == 'number' and floor(n) == n
end

local function round(n: number, d: number): number
  return floor(n * 10 ^ d) / 10 ^ d
end

local function throttle(tries: number?): number
  return wait((tries or 1) * (60 / (60 + (#Players:GetPlayers() * 10))))
end

local function wrapNull<T, R...>(value: T, ...: R...): (T | typeof(NULL_VALUE), R...)
  return type(value) ~= nil and value or NULL_VALUE, ...
end

if __DEBUG__ then
  __PING__ = true
  __VERBOSE__ = true
  __WARNINGS__ = true
elseif __VERBOSE__ then
  __WARNINGS__ = true
end

local function datastore(name: string, scope: string, ordered: boolean?, opt: DataStoreOptions?): DataStore
  if ordered then
    return DataStoreService:GetOrderedDataStore(name, scope)
  end
  return DataStoreService:GetDataStore(name, scope, opt)
end

local function deleteAsync(source: RbxDb, key: key): data?
  return wrapNull(source.datastore:RemoveAsync(key))
end

local function fetchAsync(source: RbxDb, key: key, opt: DataStoreGetOptions?): data?
  return wrapNull(source.datastore:GetAsync(key, opt))
end

local function request<A..., R...>(f: async<A..., R...>, source: RbxDb, key: key, retry: boolean, ...: A...): R...
  source.wait(key) -- wait for key to free
  source.keys[key] = tick()
  requests += 1
  local status, response
  local tries = 0
  repeat
    tries += 1
    throttle(tries)
    response = { select(2, xpcall(f, source.warn, source, key, ...)) }
  until not retry or tries == MAX_REQUEST_RETRIES or #response > 0
  -- TODO: use memory service for request fallback queue
  if tries == MAX_REQUEST_RETRIES then
    -- woops
  else
    repeat
      local null = find(response, NULL_VALUE)
      if null then
        response[null] = nil
      end
    until not null
  end
  requests -= 1
  return unpack(response)
end

local function setAsync(source: RbxDb, key: key, value: data, uids: array<number>?, opt: DataStoreSetOptions?): string
  if source.ordered then
    return wrapNull(source.datastore:SetAsync(key, value, nil, opt))
  end
  return wrapNull(source.datastore:SetAsync(key, value, uids or EMPTY, opt))
end

local function updateAsync(source: RbxDb, key: key, value: result<data, DataStoreKeyInfo>): (data, DataStoreKeyInfo)
  return wrapNull(source.datastore:UpdateAsync(key, value))
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
    assert(not self.ordered or isInt(value), `arg #1 invalid: { E_ORDERED_INT_ONLY }`)
    return request(setAsync, self, key, true, value, uids, opt)
  end

  function RbxDb.UpdateAsync(self, key, transform)
    return request(updateAsync, self, key, true, function(value, info)
      assert(not self.ordered or isInt(value), `arg #1 invalid: { E_ORDERED_INT_ONLY }`)
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
          if self.ordered and __WARNINGS__ then
            self.warn(key, W_ORDERED_SET_VERS)
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
        __mode = 'v',
      })
    end,
    __metatable = true,
    __mode = 'v',
  })

  function RbxDb.connect(name, scope, ordered, opt)
    assert(not ordered or not opt, 'arg #4 invalid: cannot pass DataStoreOptions to OrderedDataStore')
    local scope = scope or DEFAULT_SCOPE
    -- is this a typecheck hack?
    local connection: RbxDb
    connection = schema[name][scope] or setmetatable({
      datastore = datastore(name, scope, ordered, opt),
      keys = setmetatable({ }, {
        __metatable = true,
        __mode = 'v',
      }),
      locked = setmetatable({ }, {
        __metatable = true,
        __mode = 'v',
      }),
      name = name,
      ordered = ordered or false,
      scope = scope,
      -- these are probably hacks
      print = function<A...>(head: unknown, ...: A...)
        if select('#', ...) > 0 then
          print(`[{ connection }][{ head }]`, ...)
        else
          print(`[{ connection }]`, head)
        end
      end,
      wait = function(key: key)
        local elapsed = 0
        while connection.locked[key] or (tick() - (connection.keys[key] or 0)) < KEY_REQUEST_THROTTLE do
          elapsed += wait(6);
        end
        return elapsed
      end,
      warn = function<A...>(head: unknown, ...: A...)
        if select('#', ...) > 0 then
          warn(`[{ connection }][{ head }]`, ...)
        else
          warn(`[{ connection }]`, head)
        end
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

  game:BindToClose(function()
    while requests > 0 do
      wait(1)
    end
  end)
end

return RbxDb
