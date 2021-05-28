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
	
	local function clear(dict)
		for key, value in next, dict do
			if typeof(value) == 'table' then
				clear(value);
			end;
			
			dict[key] = nil;
		end;
	end;
	
	-- calculates requests per minute for unique keys
	local function rpm()
		return 60 / (60 + (#Players:GetPlayers() * 10));
	end;
	
	local function yield(t, f)		
		local elapsed = 0;

		repeat
			elapsed += RunService.Heartbeat:Wait();
		until elapsed > t + (f and f() or 0);
		
		return elapsed;
	end;
	
	function Database.Fetch(self, Key)
		local request = {
			_next = { };
			_bindings = { };
			
			_key = Key;
			_timestamp = os.clock();
		};
		
		function request.Bind(Callback)
			table.insert(request._bindings, Callback);
		end;
		
		function request.Destroy()
			clear(request);
		end;
		
		function request.Next(Callback)
			table.insert(request._next, Callback);
		end;
		
		function request.Submit()
			coroutine.wrap(function()
				local value = self:FetchAsync(Key);
				
				for _, callback in next, request._next do
					callback(value, os.clock() - request._timestamp);
				end;
			end)();
			
			for _, callback in next, request._bindings do
				callback();
			end;
			
			return request;
		end;
		
		return request;
	end;

	function Database.FetchAsync(self, Key)
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
	
	function Database.Out(self, Header, Message)
		warn(string.format('[RBXDB][%s]%s%s', self._key, Message and Header .. space or space .. Header, Message or empty));
	end;
	
	function Database.Update(self, Key, UpdateCallback)
		local request = {
			_next = { };
			_bindings = { };
			
			_key = Key;
			_submitted = false;
			_processing = false;
			_timestamp = os.clock();
			_callback = UpdateCallback;
		};

		function request.Bind(BindCallback)
			table.insert(request._bindings, BindCallback);

			return request;
		end;
		
		function request.Destroy()
			while request._processing do
				RunService.Heartbeat:Wait();
			end;
			
			clear(request);
		end;

		function request.Next(NextCallback)
			table.insert(request._next, NextCallback);

			return request;
		end;

		function request.Queue()
			if self._submitted then
				return table.find(self._requests, request);
			end;
			
			return -1;
		end;

		function request.Submit()
			table.insert(self._requests, request);
			self._submitted = true;

			for _, callback in next, request._bindings do
				callback();
			end;
			
			return request;
		end;

		return request;
	end;
	
	function Database.UpdateAsync(self, Key, UpdateCallback)
		local request = self:Update(Key, UpdateCallback).Submit();

		repeat
			RunService.Heartbeat:Wait();
		until not request.Queue();
		
		return request;
	end;
	
	function Database.Yield(self, Key)
		local keystamp = self._updates[Key];
		local throttle = keystamp and os.clock() - keystamp or 0;

		return yield(throttle, rpm);
	end;
	
	function Database.__init(DataStoreName, Scope, IsOrderedDataStore)
		local database = setmetatable({
			_busy = false;

			_fetches = { };
			_updates = { };
			_requests = { };

			_key = DataStoreName;
			_scope = Scope or '';
			_datastore = IsOrderedDataStore and DataStoreService:GetOrderedDataStore(DataStoreName, Scope)
				or DataStoreService:GetDataStore(DataStoreName, Scope);
		}, Database);

		Database.Schema[database._key] = database;

		return database;
	end;
	
	function Database.__tostring(self)
		return self._scope and string.format('`%s`{SCOPE=%s}', self._key, self._scope) or self._key;
	end;
	
	-- private table avoids cyclic references for child modules
	local modules = { };
	
	setmetatable(Database, {
		-- Constructor: <RbxDb Alias>(string DataStoreName, string Scope)
		
		__call = function(self, DataStoreName, Scope)
			return Database.__init(DataStoreName, Scope);
		end;
		
		__index = function(self, Key)
			local module = rawget(modules, Key) or rawget(self, Key);
			
			if not module then
				local moduleScript = script:FindFirstChild(Key);
				
				if moduleScript and moduleScript:IsA('ModuleScript') then
					module = require(moduleScript);
					
					rawset(modules, Key, module);
				else
					error(string.format('[RBXDB]`%s` is not a valid ModuleScript', Key));
				end;
			end;
			
			return module;
		end;
	});
	
	-- the on/off switch
	-- don't touch it
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
					
					local now = os.clock();
					local elapsed = now - request._timestamp;
					
					database._updates[request._key] = now;
					
					if __VERBOSE__ then
						warn(string.format('[RBXDB] Update request for `%s`{KEY=`%s`} took %.2fs', 
							database._key, request._key, elapsed));
					end;
					
					for index, callback in ipairs(request._next) do
						callback(elapsed);
					end;

					database:Yield(request._key);
					table.remove(database._requests, table.find(database._requests, request));
					
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
