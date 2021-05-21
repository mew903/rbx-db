-- RbxDb.Profile
-- mew903, 2021

local Database = require(script.Parent);
local ProfileDatabase = Database('RbxDb_Profile');
local SessionDatabase = Database('RbxDb_Session');

-- flags
__DEBUG__ = true;
__VERBOSE__ = true;
__TEST_PROFILE__ = false;
__AUTO_RECONCILE__ = true;

local Profile = { }; do
	Profile.__index = Profile;
	
	function Profile.Get(self, Key)
		assert(Key and typeof(Key) == 'string', 'Invalid argument to #1 (string expected)');
		
		if __DEBUG__ then
			ProfileDatabase:Out(string.format('GET `%s`{%s}', self._key, Key or nil));
		end;
		
		return self._data[Key];
	end;
	
	function Profile.Reconcile(self)
		local reconciled = { };
		
		for key, value in next, self._data do
			if typeof(self._template[key]) == 'function' then
				local result = self._template[key](self._data, value);
				
				if result == -1 then
					self._data[key] = nil;
				end;
				
				table.insert(reconciled, {
					key = key;
					old = value;
					source = self._key;
					new = self._data[key];
				});
			end;
		end;
		
		for key, value in next, self._template do
			if self._data[key] == nil then
				self._data[key] = typeof(value) == 'function' and value(self._data) or value;
				
				table.insert(reconciled, {
					key = key;
					old = nil;
					source = 'template';
					new = self._data[key];
				});
			end;
		end;
		
		if #reconciled > 0 and __VERBOSE__ then
			ProfileDatabase:Out(string.format('Profile reconciled for: %s {KEY=`%s`}', self._player.Name, self._key));
			
			for _, attribute in next, reconciled do
				ProfileDatabase:Out(string.format('\tATTR=`%s`{VALUE=`%s` -> `%s`}', attribute.key, attribute.old or 'nil', attribute.new or 'nil'));
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
	
	function Profile.Set(self, Key, Value)
		assert(Key and typeof(Key) == 'string', 'Invalid argument to #1 (string expected)');
		assert(Value, 'Invalid argument to #2 (Anything but nil expected)');
		
		if __VERBOSE__ then
			ProfileDatabase:Out(string.format('SET `%s`{%s=%s}', self._key, Key, tostring(Value)));
		end;
		
		self._data[Key] = Value;
	end;

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
		}, Profile);

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
