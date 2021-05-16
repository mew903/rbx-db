local Players = game:GetService('Players');
local RunService = game:GetService('RunService');
local DataStoreService = game:GetService('DataStoreService');

__DEBUG__ = true;

local Database = { }; do
	Database.Schema = { };
	Database.__index = Database;
	
	-- calculates requests per minute for unique keys
	local function rpm()
		return (60 + (#Players:GetPlayers() * 10)) / 60;
	end;
	
	--

	function Database.Fetch(self, Key)
		if self._fetches[Key] then
			repeat
				RunService.Heartbeat:Wait();
			until os.clock() - self._fetches[Key] > rpm();
		end;

		self._fetches[Key] = os.clock();

		return self._datastore:GetAsync(Key);
	end;
	
	function Database.Update(self, Key, UpdateCallback)
		local request = { }; do
			request._key = Key;
			request._next = { };
			request._timestamp = os.clock();
			request._callback = UpdateCallback;
		end;
		
		local chain = { }; do
			local bindings = { };
			
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
		end;

		return chain;
	end;
	
	function Database.Yield(self, Key)
		local elapsed = 0;
		local keystamp = self._timestamps[Key];
		local throttle = keystamp and os.clock() - keystamp or 0;

		repeat
			elapsed += RunService.Heartbeat:Wait();
		until elapsed > throttle + rpm();
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
				
				module = moduleScript and moduleScript:IsA('ModuleScript') and rawget(rawset(modules, Key, require(moduleScript)), Key)
					or error(string.format('`%s` is not a valid ModuleScript within RbxDb', Key));
				
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
		while running and RunService.Heartbeat:Wait() do
			for _, database in next, Database.Schema do
				for key, timestamp in next, database._timestamps do
					if os.clock() - timestamp > 10 then
						database._timestamps[key] = nil;
					end;
				end;
			end;

			local elapsed = 0;

			repeat
				elapsed += RunService.Heartbeat:Wait();
			until elapsed > 10;
		end;
	end)();
	
	-- start request runner
	coroutine.wrap(function()
		while running and RunService.Heartbeat:Wait() do
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
							if __DEBUG__ then
								warn(string.format('[DEBUG][RBXDB] Update request for `%s`{KEY=`%s`} failed. Retrying in 6 seconds...',
									database._key, request._key));
							end;

							local elapsed = 0;

							repeat
								elapsed += RunService.Heartbeat:Wait();
							until elapsed > 6;
						end;
					until success;

					database._timestamps[request._key] = os.clock();
					
					if __DEBUG__ then
						warn(string.format('[DEBUG][RBXDB] Update request for `%s`{KEY=`%s`} completed in %.6fs', 
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

		if __DEBUG__ then
			warn('[DEBUG][RBXDB] Exited successfully');
		end;
	end);
end;

return Database;
