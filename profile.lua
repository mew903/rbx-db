-- RbxDb.Profile
-- mew903, 2021

local TextService = game:GetService('TextService');

local Database = require(script.Parent);
local ProfileDatabase = Database('RbxDb_Profile');
local SessionDatabase = Database('RbxDb_Session');

-- flags
__DEBUG__ = true;
__VERBOSE__ = true;
__RESTRICT_GET__ = false;
__TEST_PROFILE__ = false;
__AUTO_RECONCILE__ = true;

local Profile = { }; do
	Profile.UserInputKeys = { };
	Profile.__index = Profile;
	
	local function copy(Input)
		local newProfile = { };

		for key, value in next, Input do
			if typeof(value) == 'table' then
				newProfile[key] = copy(value);
			else
				newProfile[key] = value;
			end;
		end;

		return newProfile;
	end;

	local function key(Player)
		return (__TEST_PROFILE__ and 'TEST_' or 'P_') .. Player.UserId;
	end;
	
	local function reconcile(Data, Template, ProfileKey, Byref, Path)
		local path = Path or '';
		
		for key, value in next, Data do
			local templateValue = Template[key];
			
			if typeof(templateValue) == 'nil' then
				Data[key] = nil;
				
				table.insert(Byref, {
					old = value;
					new = Data[key];
					key = path .. key;
					source = ProfileKey;
				});
			elseif typeof(templateValue) == 'function' then
				templateValue(Data, value);

				table.insert(Byref, {
					old = value;
					new = Data[key];
					key = path .. key;
					source = ProfileKey;
				});
			elseif typeof(templateValue) == 'table' and typeof(value) == 'table' then
				reconcile(value, templateValue, ProfileKey, Byref, key .. '/');
			end;
		end;

		for key, value in next, Template do
			if Data[key] == nil then
				local newValue;
				
				if typeof(value) == 'function' then
					newValue = value(Data);
				elseif typeof(value) == 'table' then
					newValue = copy(value);
				else
					newValue = value;
				end;
				
				Data[key] = newValue;

				table.insert(Byref, {
					old = nil;
					new = newValue;
					key = path .. key;
					source = '<template>';
				});
			end;
		end;
		
		return Byref;
	end;
	
	function Profile.AddListener(self, Listener)
		assert(Listener and typeof(Listener) == 'Instance' and Listener:IsA('Player'), 'bad argument #1 must be a Player');
		assert(not table.find(self._listeners, Listener), string.format('bad argument #1 listener already exists for profile `%s`', self._key));
		
		table.insert(self._listeners, Listener);
	end;
	
	function Profile.Get(self, Key)
		assert(Key and typeof(Key) == 'string', 'bad argument #1 must be string');
		
		if __DEBUG__ then
			ProfileDatabase:Out(string.format('GET `%s`{%s}', self._key, Key or nil));
		end;
		
		return self._data[Key];
	end;
	
	function Profile.HookToChange(self, Key, Callback)
		assert(Key and typeof(Key) == 'string', 'bad argument #1 must be string');
		assert(Callback and typeof(Callback) == 'function', 'bad argument #2 must be function');
		
		if not self._changehooks[Key] then
			self._changehooks[Key] = { };
		end;
		
		table.insert(self._changehooks[Key], Callback);
		
		return function()
			table.remove(self._changehooks[Key], table.find(self._changehooks[Key], Callback));
		end;
	end;
	
	function Profile.Reconcile(self)
		local reconciled = reconcile(self._data, self._template, self._key, { });
		
		if #reconciled > 0 and __VERBOSE__ then
			ProfileDatabase:Out(string.format('Profile reconciled for: %s {KEY=`%s`}', self._player.Name, self._key));
			
			for _, attribute in next, reconciled do
				ProfileDatabase:Out(string.format('\tATTR=`%s`{VALUE=`%s` -> `%s`}', 
					attribute.key, tostring(attribute.old) or 'nil', tostring(attribute.new) or 'nil'));
			end;
		end;
		
		return #reconciled > 0 and reconciled;
	end;
	
	function Profile.Release(self)
		local endSession = SessionDatabase:Update(self._key, function()
			return false;
		end).Next(function()
			if __DEBUG__ or __VERBOSE__ then
				SessionDatabase:Out(string.format('Profile session released for: %s {KEY=`%s`}', self._player.Name, self._key));
			end;
		end)
		
		ProfileDatabase:Update(self._key, function()
			return self._data;
		end).Next(function()
			if __DEBUG__ or __VERBOSE__ then
				ProfileDatabase:Out(string.format('Profile data saved for: %s {KEY=`%s`}', self._player.Name, self._key));
			end;
		end).Bind(function()
			endSession.Submit();
		end).Submit();
		
		return endSession.Queue;
	end;
	
	function Profile.RemoveListeners(self, Listener)
		assert(Listener, 'bad argument #1 cannot be nil');
		assert(Listener ~= self._player, 'bad argument #1 cannot be profile owner');
		assert(typeof(Listener) == 'Instance' and Listener:IsA('Player'), 'bad argument #1 must be a Player');
		
		local index = table.find(self._listeners, Listener);
		
		if index then
			return true, table.remove(self._listeners, index);
		end;
		
		return false;
	end;
	
	function Profile.Set(self, Key, Value)
		assert(Key and typeof(Key) == 'string', 'bad argument to #1 must be string');
		assert(Value, 'bad argument to #2 cannot be nil');
		
		if __VERBOSE__ then
			ProfileDatabase:Out(string.format('SET `%s`{%s=%s}', self._key, Key, tostring(Value)));
		end;
		
		if Profile.UserInputKeys[Key] then
			assert(typeof(Value) == 'string', 'bad argument to #2 value must be string');
			
			local function filter()
				local filterResult = TextService:FilterStringAsync(Value, self._player.UserId, Enum.TextFilterContext.PublicChat);
				local filteredValue = filterResult:GetNonChatStringForBroadcastAsync();
				
				Value = filteredValue;
			end;
			
			if not xpcall(filter, warn) then
				return false;
			elseif typeof(Profile.UserInputKeys[Key]) == 'function' then
				Value = Profile.UserInputKeys[Key](Value);
				
				if not Value then
					return false;
				end;
			end;
		end;
		
		self._data[Key] = Value;
		
		if self._changehooks[Key] then
			for _, callback in next, self._changehooks[Key] do
				coroutine.wrap(callback)(Value);
			end;
		end;
		
		for _, listener in next, self._listeners do
			self._onchange:FireClient(listener, Key, Value);
		end;
		
		return true;
	end;
	
	local restrictCallback;
	
	local function restrict(Player)
		if restrictCallback then
			return restrictCallback(Player);
		else
			return Player:Kick('Restricted access attempt');
		end;
	end;
	
	function Profile.setUserInputKey(Key, Callback)
		Profile.UserInputKeys[Key] = Callback or true;
	end;
	
	function Profile.setRestrictCallback(Callback)
		assert(Callback and typeof(Callback) == 'function', 'bad argument #1 must be a function');
		
		restrictCallback = Callback;
	end;
	
	function Profile.new(Player, Template)
		local isNewProfile = false;
		local timeStamp = os.time();

		local playerDataKey = key(Player);
		
		if SessionDatabase:Fetch(playerDataKey) then
			if __DEBUG__ or __VERBOSE__ then
				SessionDatabase:Out(string.format('Active session detected for `%s` {KEY=`%s`}', Player.Name, playerDataKey));
			else
				return Player:Kick('Session error. Please try again later!');
			end
		else
			SessionDatabase:Update(playerDataKey, function()
				return true;
			end).Next(function()
				if __DEBUG__ or __VERBOSE__ then
					SessionDatabase:Out(string.format('Profile session started for: %s {KEY=`%s`}', Player.Name, playerDataKey));
				end;
			end).Submit();
		end;

		local profileData = ProfileDatabase:Fetch(playerDataKey);
		
		if not profileData then
			if __VERBOSE__ then
				ProfileDatabase:Out(string.format('Profile created for `%s` {KEY=`%s`}', Player.Name, playerDataKey));
			end;
			
			isNewProfile = true;
			profileData = copy(Template);
		end;

		local profile = setmetatable({
			_player = Player;
			_data = profileData;
			_key = playerDataKey;
			_template = Template;
			
			_changehooks = { };
			_listeners = { Player };
			_onchange = Instance.new('RemoteEvent');
		}, Profile);
		
		local onChange = profile._onchange; do
			onChange.Name = 'ProfileChanged';
			onChange.Parent = Player;
		end;
		
		local access = Instance.new('RemoteFunction'); do
			access.OnServerInvoke = function(Invoker, RequestType, ...)
				local requestType = string.lower(RequestType);
				
				if requestType == 'set' then
					if Invoker ~= Player then
						return restrict(Invoker);
					end;
					
					local key, value = ...;
					assert(key and typeof(key) == 'string', 'bad argument #1 string expected');
					assert(value, 'bad argument #2 anything except nil expected');
					
					return profile:Set(key, value);
				elseif requestType == 'get' then
					if __RESTRICT_GET__ and Invoker ~= Player then
						return restrict(Invoker);
					end;
					
					local response = { };
					
					for _, key in next, { ... } do
						response[key] = profile:Get(key);
					end;
					
					return response;
				end;
			end;
			
			access.Name = 'ProfileRequest';
			access.Parent = Player;
		end;

		if __AUTO_RECONCILE__ and not isNewProfile then
			profile:Reconcile();
		end;

		return profile, isNewProfile;
	end;
	
	--
	
	Profile.Mock = { };
	Profile.Mock.__index = Profile.Mock;
	
	function Profile.Mock.Get(self, Key)
		if __VERBOSE__ then
			ProfileDatabase:Out('MOCK', string.format('FETCH `%s`{%s}', self._key, Key));
		end;
		
		return self._data[Key];
	end;
	
	function Profile.Mock.Reconcile(self)
		-- pass
		if __VERBOSE__ then
			ProfileDatabase:Out('MOCK', string.format('Profile reconciled for: %s {KEY=`%s`}', self._player.Name, self._key));
		end;
	end;
	
	function Profile.Mock.Release(self)
		-- pass
		if __DEBUG__ or __VERBOSE__ then
			SessionDatabase:Out('MOCK', string.format('Profile session released for: %s {KEY=`%s`}', self._player.Name, self._key));
		end;
	end;
	
	function Profile.Mock.Set(self, Key, Value)
		if __VERBOSE__ then
			ProfileDatabase:Out('MOCK', string.format('SET `%s`{%s=%s}', self._key, Key, Value));
		end;
		
		self._data[Key] = Value;
	end;
	
	function Profile.Mock.new(Player, Template)
		local mockKey = key(Player);
		
		if __DEBUG__ or __VERBOSE__ then
			SessionDatabase:Out('MOCK', string.format('Profile session started for: %s {KEY=`%s`}', Player.Name, mockKey));
			ProfileDatabase:Out('MOCK', string.format('Profile session started for: %s {KEY=`%s`}', Player.Name, mockKey));
		end;
		
		return setmetatable({
			_key = mockKey;
			_player = Player;
			_template = Template;
			_data = copy(Template);
		}, Profile.Mock), true;
	end;
end;

return Profile;
