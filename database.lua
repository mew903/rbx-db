-- RbxDb
-- mew903, 2021

-- flags
__DEBUG__ = true;
__VERBOSE__ = true;

local Players = game:GetService('Players');
local RunService = game:GetService('RunService');
local DataStoreService = game:GetService('DataStoreService');

local Database = { }; do
	Database.Schema = { };
	Database.__index = Database;
	
	-- calculates requests per minute for unique keys
	local function rpm()
		return (60 + (#Players:GetPlayers() * 10)) / 60;
	end;
	
	local function yield(t, f)		
		local elapsed = 0;

		repeat
			elapsed += RunService.Heartbeat:Wait();
		until elapsed > t + (f and f() or 0);
	end;
	
	--

	function Database.Fetch(self, Key)
		if self._fetches[Key] then
			repeat
				RunService.Heartbeat:Wait();
			until os.clock() - self._fetches[Key] > rpm();
		end;
		
		local value; do
			local function fetchValue()
				value = self._datastore:GetAsync(Key);
			end;
			
			repeat
				local success = xpcall(fetchValue, warn);
				
				if not success then
					if __DEBUG__ or __VERBOSE__ then
						warn(string.format('[DEBUG][RBXDB] Fetch request for `%s`{KEY=`%s`} failed. Retrying in 6 seconds...', self._key, Key));
					end;
					
					yield(6);
				end;
			until success;
		end;

		self._fetches[Key] = os.clock();

		return value;
	end;
	
	function Database.Update(self, Key, UpdateCallback)
		local chain, bindings, request = { }, { }, {
			_key = Key;
			_next = { };
			_timestamp = os.clock();
			_callback = UpdateCallback;
		};

		function chain.Bind(BindCallback)
			table.insert(bindings, BindCallback);

			return chain;
		end;

		function chain.Next(NextCallback)
			table.insert(request._next, NextCallback);

			return chain;
		end;

		function chain.Queue()
			return table.find(self._updates, request);
		end;

		function chain.Submit()
			table.insert(self._updates, request);

			for _, callback in next, bindings do
				callback();
			end;
		end;

		return chain;
	end;
	
	function Database.Yield(self, Key)
		local keystamp = self._timestamps[Key];
		local throttle = keystamp and os.clock() - keystamp or 0;

		yield(throttle, rpm);
	end;
	
	--
	
	function Database.SetDebugMode(DebugMode)
		__DEBUG__ = __DEBUG__ or DebugMode;
	end;
	
	function Database.new(DataStoreName, Scope)
		local database = setmetatable({
			_busy = false;
			
			_fetches = { };
			_updates = { };
			_timestamps = { };
			
			_datastore = DataStoreService:GetDataStore(DataStoreName, Scope);
			_key = Scope and string.format('%s_%s', DataStoreName, Scope) or DataStoreName;
		}, Database);
		
		Database.Schema[database._key] = database;
		
		return database;
	end;
	
	-- private table avoids cyclic references for child modules
	local modules = { };
	
	setmetatable(Database, {
		__call = function(self, DataStoreName, Scope)
			return Database.new(DataStoreName, Scope);
		end;
		
		__index = function(self, Key)
			local module = rawget(modules, Key);
			
			if not module then
				local moduleScript = script:FindFirstChild(Key);
				module = moduleScript and moduleScript:IsA('ModuleScript') and rawget(rawset(modules, Key, require(moduleScript)), Key);
				
				assert(module, string.format('`%s` is not a valid ModuleScript within RbxDb', Key));
				
				if __DEBUG__ and typeof(module.SetDebugMode) == 'function' then
					module:SetDebugMode(__DEBUG__);
				end;
			end;
			
			return module;
		end;
	});
	
	-- the on/off switch
	local running = true;
	
	-- stale timestamp janitor
	coroutine.wrap(function()
		while running do
			for _, database in next, Database.Schema do
				for key, timestamp in next, database._timestamps do
					if os.clock() - timestamp > 10 then
						database._timestamps[key] = nil;
					end;
				end;
			end;

			yield(10);
		end;
	end)();
	
	-- start request runner
	coroutine.wrap(function()
		if __VERBOSE__ then
			warn('[VERBO][RBXDB] Starting RbxDb')
		end;
		
		while running do
			for _, database in next, Database.Schema do
				if database._busy or #database._updates == 0 then
					continue;
				end;
				
				database._busy = true;
				
				coroutine.wrap(function()
					local request = database._updates[1];

					repeat
						local success = xpcall(function()
							database._datastore:UpdateAsync(request._key, request._callback);
						end, warn);

						if not success then
							if __DEBUG__ or __VERBOSE__ then
								warn(string.format('[DEBUG][RBXDB] Update request for `%s`{KEY=`%s`} failed. Retrying in 6 seconds...',
									database._key, request._key));
							end;

							yield(6);
						end;
					until success;

					database._timestamps[request._key] = os.clock();
					
					if __VERBOSE__ then
						warn(string.format('[VERBO][RBXDB] Update request for `%s`{KEY=`%s`} completed in %.6fs', 
							database._key, request._key, database._timestamps[request._key] - request._timestamp));
					end;
					
					for index, callback in ipairs(request._next) do
						callback();

						request._next[index] = nil;
					end;

					database:Yield(request._key);
					table.remove(database._updates, table.find(database._updates, request));
					
					for key in next, request do
						request[key] = nil;
					end;
					
					request = nil;
					database._busy = false;
				end)();
			end;
			
			RunService.Heartbeat:Wait();
		end;
	end)();

	-- keep the server from closing before all requests have been processed
	game:BindToClose(function()
		repeat
			local busy = false;
			
			for _, database in next, Database.Schema do
				busy = database._busy or #database._updates > 0;
				
				if busy then
					break;
				end;
			end;
			
			RunService.Heartbeat:Wait();
		until busy == false;

		running = false;

		if __VERBOSE__ then
			warn('[VERBO][RBXDB] Exited successfully');
		end;
	end);
end;

return Database;
