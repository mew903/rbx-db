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
		return 60 / (60 + (#Players:GetPlayers() * 10));
	end;
	
	local function yield(t, f)		
		local elapsed = 0;

		repeat
			elapsed += RunService.Heartbeat:Wait();
		until elapsed > t + (f and f() or 0);
	end;

	function Database.Fetch(self, Key)
		if self._fetches[Key] then
			repeat
				RunService.Heartbeat:Wait();
			until os.clock() - self._fetches[Key] > rpm();
		end;
		
		local value; do
			local function get()
				value = self._datastore:GetAsync(Key);
			end;
			
			repeat
				local success = xpcall(get, warn);
				
				if not success then
					if __DEBUG__ or __VERBOSE__ then
						warn(string.format('[RBXDB] Fetch request for `%s`{KEY=`%s`} failed. Retrying in 6 seconds...', self._key, Key));
					end;
					
					yield(6);
				end;
			until success;
			
			self._fetches[Key] = os.clock();
		end;
		
		return value;
	end;
	
	local empty, space = '', ' ';
	
	function Database.Out(self, header, message)
		warn(string.format('[RBXDB][%s]%s%s', self._key, message and header .. space or space .. header, message or empty));
	end;
	
	function Database.Update(self, Key, UpdateCallback)
		local chain, bindings, request = { }, { }, {
			_key = Key;
			_next = { };
			_submitted = false;
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
			if self._submitted then
				return table.find(self._requests, request);
			end;
			
			return -1;
		end;

		function chain.Submit()
			table.insert(self._requests, request);
			self._submitted = true;

			for _, callback in next, bindings do
				callback();
			end;
		end;

		return chain;
	end;
	
	function Database.Yield(self, Key)
		local keystamp = self._updates[Key];
		local throttle = keystamp and os.clock() - keystamp or 0;

		yield(throttle, rpm);
	end;
	
	-- private table avoids cyclic references for child modules
	local modules = { };
	
	setmetatable(Database, {
		-- Constructor: <module alias>(string DataStoreName, string Scope)
		
		__call = function(self, DataStoreName, Scope)
			local database = setmetatable({
				_busy = false;
				
				_fetches = { };
				_updates = { };
				_requests = { };
				
				_datastore = DataStoreService:GetDataStore(DataStoreName, Scope);
				_key = Scope and string.format('%s@%s', DataStoreName, Scope) or DataStoreName;
			}, Database);
			
			Database.Schema[database._key] = database;

			return database;
		end;
		
		__index = function(self, Key)
			local module = rawget(modules, Key);
			
			if not module then
				local moduleScript = script:FindFirstChild(Key);
				module = moduleScript and moduleScript:IsA('ModuleScript') and rawget(rawset(modules, Key, require(moduleScript)), Key);
				assert(module, string.format('[RBXDB]`%s` is not a valid ModuleScript', Key));
			end;
			
			return module;
		end;
	});
	
	-- the on/off switch
	local running = true;
	
	-- stale timestamp janitor
	coroutine.wrap(function()
		local function clockOut()
			return not running and -10 or 0;
		end;
		
		local function sweep(TimestampTable, CleanAfter)
			for key, timestamp in next, TimestampTable do
				if os.clock() - timestamp > CleanAfter then
					TimestampTable[key] = nil;
				end;
			end;
		end;
		
		local sweepFrequency = 10;
		
		while running do
			for _, database in next, Database.Schema do
				coroutine.wrap(function()
					sweep(database._fetches, sweepFrequency);
					sweep(database._updates, sweepFrequency);
				end)();
			end;

			yield(10, clockOut);
		end;
	end)();
	
	-- start request runner
	coroutine.wrap(function()
		if __VERBOSE__ then
			warn('[RBXDB] Starting RbxDb')
		end;
		
		while running do
			for _, database in next, Database.Schema do
				if database._busy or #database._requests == 0 then
					continue;
				end;
				
				database._busy = true;
				
				coroutine.wrap(function()
					local request = database._requests[1];
					
					local function update()
						database._datastore:UpdateAsync(request._key, request._callback);
					end;

					repeat
						local success = xpcall(update, warn);

						if not success then
							if __DEBUG__ or __VERBOSE__ then
								warn(string.format('[RBXDB] Update request for `%s`{KEY=`%s`} failed. Retrying in 6 seconds...',
									database._key, request._key));
							end;

							yield(6);
						end;
					until success;
					
					database._updates[request._key] = os.clock();
					
					if __VERBOSE__ then
						warn(string.format('[RBXDB] Update request for `%s`{KEY=`%s`} took %.2fs', 
							database._key, request._key, database._updates[request._key] - request._timestamp));
					end;
					
					for index, callback in ipairs(request._next) do
						callback();

						request._next[index] = nil;
					end;

					database:Yield(request._key);
					table.remove(database._requests, table.find(database._requests, request));
					
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
				busy = database._busy or #database._requests > 0;
				
				if busy then
					break;
				end;
			end;
			
			RunService.Heartbeat:Wait();
		until busy == false;

		running = false;

		if __VERBOSE__ then
			warn('[RBXDB] Exited successfully');
		end;
	end);
end;

return Database;
