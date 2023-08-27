local Knit = require(game:GetService("ReplicatedStorage").Packages.Knit)

local ClanService = Knit.CreateService({
	Name = "ClanService",
	Client = {
		MembersChanged = Knit.CreateSignal(),
		PlayerAddedToClan = Knit.CreateSignal(),
		PlayerRemovedFromClan = Knit.CreateSignal(),
		InvitesChanged = Knit.CreateSignal(),
		JoinRequestsChanged = Knit.CreateSignal(),
		EditInfoChanged = Knit.CreateSignal(),
		InfoChanged = Knit.CreateSignal(),
		ClanInvitesChanged = Knit.CreateSignal(),
		TasksChanged = Knit.CreateSignal(),
		TaskResetsChanged = Knit.CreateSignal(),
		LevelChanged = Knit.CreateSignal(),
		TopClans = Knit.CreateProperty(),
		RecommendedClans = Knit.CreateProperty(),
	}
})

local Players = game:GetService("Players")
local MessagingService = game:GetService("MessagingService")
local HttpService = game:GetService("HttpService")
local LocalizationService = game:GetService("LocalizationService")
local UserService = game:GetService("UserService")
local TextService = game:GetService("TextService")
local RunService = game:GetService("RunService")

local Util = Knit.Util
local Timer = require(Util.Timer)
local TableUtil = require(Util.TableUtil)
local Signal = require(Util.Signal)
local Promise = require(Util.Promise)

local Modules = Knit.Modules
local ClanClass = require(Modules.ClanClass)
local SafeDataStore = require(Modules.SafeDataStore)
local SafeMemoryStore = require(Modules.SafeMemoryStore)
local BitBuffer = require(Modules.Bitbuf)
local Ascii85 = require(Modules.Ascii85)

local Shared = Knit.Shared
local RateLimit = require(Shared.RateLimit)
local ExtraUtil = require(Shared.Util)
local PetUtil = ExtraUtil.Get("PetUtil")

local List = Knit.List
local ClansList = List.Get("Clans")
local clanTasks = ClansList.Tasks
local dailyClanTasks = clanTasks.Daily
local weeklyClanTasks = clanTasks.Weekly

local SERVER_ID = HttpService:GenerateGUID(false)

local DataService = nil
local ChatService = nil
local CurrencyService = nil
local GemCollectorService = nil

local cachedClans = {}
local cachedInvites = {}
local cachedJoinData = {}
local cachedTopClans = {}
local cachedRecommendedClans = {}
local clansToSync = {}

local CLAN_PRICE = 10_000

local rateLimiter = RateLimit.NewRateLimiter(2)

local JOIN_DATA_TEMP = {
	lastClanAccepted = nil,
	requests = {},
	lastPetPower = 0,
	lastRebirth = 1,
	country = "SS",
}

local inboundInvitesDataStore = SafeDataStore.createDataStore("Invites6")
local outboundJoinDataDataStore = SafeDataStore.createDataStore("JoinData9")
local topClansOrderedDataStore = SafeDataStore.createOrderedDataStore("TopClans1")

local queueDuration = 7

local recommendedClans = SafeMemoryStore.getQueue("RecommendedClans3")

local CustomWriteQueue = {}
local function CustomWriteQueueAsync(callback, store, key)
	if CustomWriteQueue[store] == nil then
		CustomWriteQueue[store] = {}
	end

	if CustomWriteQueue[store][key] == nil then
		CustomWriteQueue[store][key] = {
			LastWrite = 0,
			Queue = {},
			CleanupJob = nil
		}
	end

	local queue_data = CustomWriteQueue[store][key]
	local queue = queue_data.Queue

	if os.clock() - queue_data.LastWrite > queueDuration and #queue == 0 then
		queue_data.LastWrite = os.clock()
		return callback()
	else
		table.insert(queue, callback)
		while true do
			if os.clock() - queue_data.LastWrite > queueDuration and queue[1] == callback then
				table.remove(queue, 1)
				queue_data.LastWrite = os.clock()
				return callback()
			end
			task.wait()
		end
	end
end

local function DeepCopyTable(t)
	local copy = {}
	for key, value in pairs(t) do
		if type(value) == "table" then
			copy[key] = DeepCopyTable(value)
		else
			copy[key] = value
		end
	end
	return copy
end

local function checkString(userId, string, maxLength, minLength)
	minLength = minLength or 2

	if not tostring(string) then
		return false
	end

	if #string > maxLength then
		return false, "is too long"
	end

	if #string < minLength then
		return false, "is too short"
	end

	local success, result = pcall(function()
		return TextService:FilterStringAsync(string, userId, Enum.TextFilterContext.PrivateChat)
	end)

	return success, "couldn't be checked. Try again!"
end

function ClanService:KnitStart()
	self.updateConnection = Signal.new()
	
	DataService.DataReset:Connect(function(player)
		-- If they have a clan in their join data and their data was reset, put them back into their clan
		
		local userId = player.UserId
		
		local joinData = self:GetOutboundClanJoinData(userId)
		if not joinData then
			return
		end
		
		local lastClanAccepted = joinData.lastClanAccepted
		if lastClanAccepted then
			self:SetClanInProfile(player, lastClanAccepted)
		end
	end)
	
	Players.PlayerAdded:Connect(function(player)
		local userId = player.UserId
		local clanName = self:GetClanInProfile(player)

		local joinData = self:GetOutboundClanJoinData(userId)
		if not joinData then
			return
		end

		local lastClanAccepted = joinData.lastClanAccepted
		if lastClanAccepted and not clanName then
			self:SetClanInProfile(player, lastClanAccepted)
		end

		--[[
		If they have a clan or have an accpeted clan but they arent in the clans member list,
		then remove that because they were most likely removed offline
		]]

		local clanInOrAcceptedInto = clanName or lastClanAccepted
		if not clanInOrAcceptedInto then
			return
		end

		local clanInfo = self:GetClanInfo(clanInOrAcceptedInto)
		if not clanInfo or not clanInfo.Members[userId] then
			print("Removed clan from profile")
			self:SetClanInProfile(player, nil)
			
			local joinData = self:GetOutboundClanJoinData(userId)
			if not joinData then
				return
			end

			joinData.lastClanAccepted = nil
			
			ClanService:PublishAsync("JoinData", {
				Data = joinData,
				User = userId,
				SetOn = SERVER_ID,
			})

			self:SetJoinDataAsync(userId, joinData)
		end
		
		local newClanName = self:GetClanInProfile(player)
		if newClanName then
			ChatService:JoinChannel(player.Name, newClanName)
		end
	end)

	Timer.Simple(5, function()
		local clansCheckedInPeriod = {}

		for _, player in Players:GetPlayers() do
			local clanName = self:GetClanInProfile(player)
			if not clanName then
				continue
			end

			if clansCheckedInPeriod[clanName] then
				continue
			end
			clansCheckedInPeriod[clanName] = true

			local cachedClan = self:WaitForCachedClan(clanName)
			if not cachedClan then
				continue
			end

			local clanInfo = cachedClan.Info

			local lastTaskResets = clanInfo.LastTaskResets
			local tasks = clanInfo.Tasks

			local requiredResets = {}

			for taskDurationType, lastResetTime in lastTaskResets do
				local timePassed = os.time() - lastResetTime
				local timeRequired = ClansList.Timers[taskDurationType]

				if timePassed < timeRequired then
					continue
				end

				requiredResets[taskDurationType] = true
			end

			local dailyRequired = requiredResets.Daily
			local weeklyRequired = requiredResets.Weekly

			local wasReset = dailyRequired or weeklyRequired

			if dailyRequired then
				local dailyTasks = tasks.Daily

				cachedClan:ClearTasks("Daily")

				for i = 1, 3 do
					local chosenTaskKey = math.random(1, #dailyClanTasks)
					local newTask = dailyClanTasks[chosenTaskKey]

					local newTaskTemplate = {
						Key = chosenTaskKey,
						Completed = false,
						Current = 0,
					}

					cachedClan:AddTask("Daily", newTaskTemplate)
				end

				cachedClan:ResetTaskTimer("Daily")
			end

			if weeklyRequired then
				local dailyTasks = tasks.Weekly

				cachedClan:ClearTasks("Weekly")

				for i = 1, 2 do
					local chosenTaskKey = math.random(1, #weeklyClanTasks)
					local newTask = weeklyClanTasks[chosenTaskKey]

					local newTaskTemplate = {
						Key = chosenTaskKey,
						Completed = false,
						Current = 0,
					}

					cachedClan:AddTask("Weekly", newTaskTemplate)
				end

				cachedClan:ResetTaskTimer("Weekly")
			end

			if wasReset then
				self:AddClanToSync(cachedClan, {
					LastTaskResets = true,
					Tasks = true,
				})
			end
		end
	end)

	local lastDataStoreSync = os.time()

	Timer.Simple(1, function()
		for clanName, syncInfo in clansToSync do
			task.defer(function()
				local cachedClan = self:WaitForCachedClan(clanName, true)
				if not cachedClan then
					return
				end

				self:PublishToMessagingService(clanName, syncInfo)

				task.defer(CustomWriteQueueAsync, function()
					cachedClan:Sync()
				end, "Clans", clanName)

				if os.time() - lastDataStoreSync > 7 then
					lastDataStoreSync = os.time()

				end

				self:UpdateClientsFromSyncInfo(clanName, syncInfo, cachedClan.Info)
			end)
		end

		clansToSync = {}
	end)

	Timer.Simple(10, function()
		for _, player in Players:GetPlayers() do
			local profile, replica = DataService:GetUserData(player)

			local clanName = profile.clan
			if not clanName then
				continue
			end

			local cachedClan = self:GetCachedClan(clanName)
			if not cachedClan then
				continue
			end

			local clanInfo = cachedClan.Info
			local members = clanInfo.Members
			local userId = player.UserId

			local memberInfo = members[userId]
			if not memberInfo then
				continue
			end

			local petPower = PetUtil.GetTotalPetPower(player)
			if not petPower then
				continue
			end

			local success, code = pcall(LocalizationService.GetCountryRegionForPlayerAsync, LocalizationService, player)
			local country = if success and code then code else "SS"
			
			local oldestCollect = os.time()
			for worldName, _ in profile.lastCollects do
				local secondsUntilFull = GemCollectorService:GetSecondsUtilFull(player, worldName) 
				if secondsUntilFull > oldestCollect then
					continue
				end
				
				oldestCollect = secondsUntilFull
			end
			
			cachedClan:SetMember(userId, {
				Rank = memberInfo.Rank,
				PetPower = petPower,
				Rebirth = profile.rebirths,
				JoinTime = memberInfo.JoinTime,
				CollectTime = os.time() + oldestCollect,
				Country = if country then country else memberInfo.Country,
			})

			self:AddClanToSync(cachedClan, {
				Members = true,
			})

			local joinData = self:GetOutboundClanJoinData(userId)
			if not joinData then
				continue
			end

			joinData.lastPetPower = petPower
			joinData.lastRebirth = profile.rebirths

			if country then
				joinData.country = country
			end

			self:SetJoinDataAsync(userId, joinData)
		end
	end)	

	local function refreshTopClans()
		for _, player in Players:GetPlayers() do
			local profile, replica = DataService:GetUserData(player)

			local clanName = profile.clan
			if not clanName then
				continue
			end

			local cachedClan = self:GetCachedClan(clanName)
			if not cachedClan then
				continue
			end

			local clanInfo = cachedClan.Info

			local petPower = 0
			for _, memberInfo in clanInfo.Members do
				petPower += memberInfo.PetPower
			end

			task.defer(CustomWriteQueueAsync, function()
				SafeDataStore.setData(
					topClansOrderedDataStore,
					clanName,
					petPower
				)
			end, "TopClans", clanName)

			task.wait(7)
		end

		self:RefreshTopClans()
	end

	task.defer(refreshTopClans)
	Timer.Simple(120, refreshTopClans)

	local function refreshRecommendedClans()
		local status, clansInQueue, id = SafeMemoryStore.readKeys(recommendedClans, 10):await()
		if not status or not clansInQueue then
			return
		end

		for _, clan in clansInQueue do
			if table.find(cachedRecommendedClans, clan) then
				continue
			end

			local clanInfo = self:GetClanInfo(clan)
			if not clanInfo then
				continue
			end

			local memberCount = 0
			for _, _ in clanInfo.Members do
				memberCount += 1
			end

			if memberCount >= 15 then
				continue
			end

			table.insert(cachedRecommendedClans, clan)
		end

		for index, clan in cachedRecommendedClans do
			local clanInfo = self:GetClanInfo(clan)
			if not clanInfo then
				table.remove(cachedRecommendedClans, index)
				continue
			end

			local memberCount = 0
			for _, _ in clanInfo.Members do
				memberCount += 1
			end

			if memberCount >= 15 then
				table.remove(cachedRecommendedClans, index)
				continue
			end
		end

		self.Client.RecommendedClans:Set(cachedRecommendedClans)

		SafeMemoryStore.removeKeys(recommendedClans, id)
	end

	task.defer(refreshRecommendedClans)
	Timer.Simple(20, refreshRecommendedClans)

	MessagingService:SubscribeAsync("Clans", function(messagingData)
		local Ascii85Encoded = messagingData.Data
		local bitbufString = Ascii85.decode(Ascii85Encoded)

		local newBitBuf = BitBuffer.fromString(bitbufString)
		local clanData = {}

		local function readString()
			local length = newBitBuf:ReadByte()
			return newBitBuf:ReadBytes(length)
		end

		clanData.SetOn = readString()

		local clanName = readString()

		clanData.Name = clanName

		local clanTag = readString()
		local clanDescription = readString()

		local info = {}
		clanData.Info = info

		info.Name = clanName
		info.Tag = clanTag
		info.Description = clanDescription

		local memberCount = newBitBuf:ReadByte()

		info.Members = {}

		for i = 1, memberCount do
			local userId = newBitBuf:ReadFloat(64)
			local country = readString()
			local petPower = newBitBuf:ReadFloat(64)
			local rank = newBitBuf:ReadByte()
			local rebirth = newBitBuf:ReadFloat(64)

			info.Members[userId] = {
				Country = country,
				PetPower = petPower,
				Rank = rank,
				Rebirth = rebirth
			}
		end

		local joinRequestCount = newBitBuf:ReadFloat(64)

		info.JoinRequests = {}

		for i = 1, joinRequestCount do
			local userId = newBitBuf:ReadFloat(64)
			info.JoinRequests[userId] = true
		end

		local inviteCount = newBitBuf:ReadFloat(64)

		info.Invites = {}

		for i = 1, inviteCount do
			local userId = newBitBuf:ReadFloat(64)
			info.Invites[userId] = true
		end

		info.IsPublic = newBitBuf:ReadBool()
		info.IsDestroyed = newBitBuf:ReadBool()

		local levelData = {}
		info.Level = levelData

		levelData.Level = newBitBuf:ReadByte()
		levelData.Exp = newBitBuf:ReadFloat(64)

		local lastTaskResets = {}
		info.LastTaskResets = lastTaskResets

		lastTaskResets.Daily = newBitBuf:ReadFloat(64)
		lastTaskResets.Weekly = newBitBuf:ReadFloat(64)

		local tasks = {}
		info.Tasks = tasks

		local dailyTasks = {}
		tasks.Daily = dailyTasks

		local dailyTasksCount = newBitBuf:ReadByte()
		local weeklyTasksCount = newBitBuf:ReadByte()

		for i = 1, dailyTasksCount do
			local key = readString()

			dailyTasks[key] = {
				Key = newBitBuf:ReadFloat(64),
				Completed = newBitBuf:ReadBool(),
				Current = newBitBuf:ReadFloat(64)
			}
		end

		local weeklyTasks = {}
		tasks.Weekly = weeklyTasks

		for i = 1, weeklyTasksCount do
			local key = readString()

			weeklyTasks[key] = {
				Key = newBitBuf:ReadFloat(64),
				Completed = newBitBuf:ReadBool(),
				Current = newBitBuf:ReadFloat(64)
			}
		end

		local emblemData = {}
		info.Emblem = emblemData

		emblemData.color = {
			newBitBuf:ReadByte(),
			newBitBuf:ReadByte(),
			newBitBuf:ReadByte()
		}

		emblemData.image = readString()
		emblemData.decal = readString()

		local changed = {}
		clanData.Changed = changed

		local amountOfChanges = newBitBuf:ReadByte()

		for i = 1, amountOfChanges do
			local changeType = readString()
			local changeValueIfFloat = newBitBuf:ReadFloat(64)
			local isFloat = changeValueIfFloat ~= 0
			local changedValueIfNotFloat = newBitBuf:ReadBool()

			changed[changeType] = if isFloat then changeValueIfFloat else changedValueIfNotFloat
		end
		
		--[[for changeType, changeValue in changed do
		writeString(changeType)
		
		local hasFloat = changeType == "MemberAdded" or changeType == "MemberRemoved"
		newBitbuf:WriteFloat(64, if hasFloat then changeValue else 0)
		newBitbuf:WriteBool(not hasFloat)
	end]]
		
		-- We don't want double requests since the publish is sent to its own server too
		if clanData.SetOn == SERVER_ID then
			return
		end

		print(clanData, "CLAN DATA RETRIEVED")

		local updatedInfoKeys = clanData.Info
		local clanName = updatedInfoKeys.Name

		-- We want all clans that have requests to be cached on all servers so they can be easily read
		local cachedClan = self:GetCachedClan(clanName)
		if not cachedClan then
			return
		end

		local newClanInfo = clanData.Info

		cachedClan.Info = newClanInfo

		--[[local clanInfo = cachedClan.Info
		for key, data in clanData.Changed do
			clanInfo[key] = data
		end]]

		cachedClan:Update(newClanInfo)

		self:UpdateClientsFromSyncInfo(clanName, clanData.Changed, newClanInfo)
	end)

	MessagingService:SubscribeAsync("Invites", function(data)
		local inviteData = data.Data

		if inviteData.SetOn == SERVER_ID then
			return
		end

		local userInvited = inviteData.UserInvited

		local playerInServer = Players:GetPlayerByUserId(userInvited)
		if not playerInServer then
			return
		end

		local cachedInvites = self:GetInvites(playerInServer.UserId)
		if not cachedInvites then
			return
		end
		
		cachedInvites[inviteData.Clan] = true

		self.Client.InvitesChanged:Fire(playerInServer, cachedInvites)
	end)

	MessagingService:SubscribeAsync("JoinData", function(data)
		local newJoinData = data.Data

		if newJoinData.SetOn == SERVER_ID then
			return
		end

		local joinData = cachedJoinData[newJoinData.User]
		if not joinData then
			return
		end

		cachedJoinData[newJoinData.User] = newJoinData.Data
	end)

	game:BindToClose(function()
		if RunService:IsStudio() then
			queueDuration = 0.1
			task.wait(2)
		else
			local keysRunning = 0
			for _, store in CustomWriteQueue do
				for _, key in store do
					for _, _ in key.Queue do
						keysRunning += 1
					end
				end
			end
			
			task.wait(math.min(keysRunning * 7, 30))
		end
	end)
end

function ClanService:KnitInit()
	DataService = Knit.GetService("DataService")
	ChatService = Knit.GetService("ChatService")
	CurrencyService = Knit.GetService("CurrencyService")
	GemCollectorService = Knit.GetService("GemCollectorService")
end

function ClanService.Client:FilterStringAsync(player, string)
	local success, result = pcall(function()
		return TextService:FilterStringAsync(string, player.UserId)
	end)
	
	return success, result
end

function ClanService.Client:RequestClanCreation(player, ...)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:CreateClan(player, ...)
end

function ClanService.Client:RequestClanInfo(player, clanName)
	local clanInfo = self.Server:GetClanInfo(clanName)
	return clanInfo
end

function ClanService.Client:RequestClanInfoIfExists(player, clanName)
	local cachedClan = self.Server:GetCachedClan(clanName)
	if not cachedClan then
		if not ClanClass.GetClanInfoRaw(clanName) then
			return
		end
	end

	local clanInfo = self.Server:GetClanInfo(clanName)
	if not clanInfo then
		return
	end

	return clanInfo
end

function ClanService.Client:RequestSendInvite(player, playerRequestingUsername)
	local canProcess = rateLimiter:CheckRate(playerRequestingUsername)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:SendInvite(player, playerRequestingUsername)
end

function ClanService.Client:RequestGetInvites(player)
	return self.Server:GetInvites(player.UserId)
end

function ClanService.Client:RequestAcceptInvite(player, clanName)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:AcceptInvite(player, clanName)
end

function ClanService.Client:RequestRejectInvite(player, clanName)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:RejectClanInvite(player, clanName)
end

function ClanService.Client:RequestLeaveClan(player)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:LeaveClan(player)
end

function ClanService.Client:RequestJoinClanRequest(player, clanName)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:JoinClanRequest(player, clanName)
end

function ClanService.Client:RequestAcceptJoinRequest(player, userId)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:AcceptJoinRequest(player, userId)
end

function ClanService.Client:RequestDenyJoinRequest(player, userId)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:DenyJoinRequest(player, userId)
end

function ClanService.Client:RequestJoinData(player, userId)
	return self.Server:GetOutboundClanJoinData(userId or player.UserId)
end

function ClanService.Client:GetClanInProfile(player, playerSelected)
	return self.Server:GetClanInProfile(playerSelected or player)
end

function ClanService.Client:GetRecommendedClans(player)
	return cachedRecommendedClans
end

function ClanService.Client:RequestClanEdit(player, editiedData)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:ClanEdit(player, editiedData)
end

function ClanService.Client:RequestPromoteMember(player, userId)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:PromoteMember(player, userId)
end

function ClanService.Client:RequestDemoteMember(player, userId)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return self.Server:DemoteMember(player, userId)
end

function ClanService.Client:RequestTopClans(player)
	local canProcess = rateLimiter:CheckRate(player)
	if not canProcess then
		return false, "You are doing this too fast!"
	end

	return cachedTopClans
end

function ClanService:RefreshTopClans()
	local status, pages = SafeDataStore.getSortedData(topClansOrderedDataStore, false, 100):await()
	if not status then
		return
	end

	local topClans = {}

	while true do
		for index, item in ipairs(pages:GetCurrentPage()) do
			topClans[index] = item
		end

		if pages.IsFinished or #topClans >= 500 then
			break
		end

		local success, err = pcall(function()
			pages:AdvanceToNextPageAsync()
		end)

		if not success then
			warn(err)
			break
		end
	end

	table.clear(cachedTopClans)

	for place, data in topClans do
		local clanName = data.key
		local petPower = data.value

		local cachedClan = self:WaitForCachedClan(clanName)
		if not cachedClan then
			continue
		end

		table.insert(cachedTopClans, clanName)
	end

	self.Client.TopClans:Set(cachedTopClans)
end

function ClanService:UpdateTasks(player, taskType, value)
	local clanName = self:GetClanInProfile(player)
	if not clanName then
		return
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	local clanInfo = cachedClan.Info
	local tasks = clanInfo.Tasks

	local wasChanged = false
	local wasCompleted = false

	for refreshType, taskDatas in tasks do
		for id, taskData in taskDatas do
			local taskInfo = clanTasks[refreshType][taskData.Key]

			if taskInfo.Type ~= taskType then
				continue
			end

			if taskData.Completed then
				continue
			end

			wasChanged = true

			taskData.Current = math.min(
				taskData.Current + value,
				taskInfo.Total
			)

			if taskData.Current == taskInfo.Total then
				wasCompleted = true

				local levelData = clanInfo.Level

				local function checkLevel(newExp, level)
					local levelInfo = ClansList.Levels[level]
					local lastLevelValue = if level > 1 then ClansList.Levels[level - 1].Value else 0

					if not levelInfo then
						return newExp, level
					end

					local neededExp = (levelInfo.Value - lastLevelValue)

					if newExp < neededExp then
						return newExp, level
					end

					return checkLevel(newExp - neededExp, level + 1)
				end

				local newExp, newLevel = checkLevel(levelData.Exp + taskInfo.Reward, levelData.Level)

				cachedClan:SetLevel({
					Level = newLevel,
					Exp = newExp,
				})

				taskData.Completed = true
			end

			cachedClan:UpdateTask(refreshType, id, taskData)
		end
	end

	if wasCompleted or wasChanged then
		local syncData = {}

		if wasChanged then
			syncData.Tasks = true
		end

		if wasCompleted then
			syncData.Level = true
		end		

		self:AddClanToSync(cachedClan, syncData)
	end
end

function ClanService:UpdateClientsFromSyncInfo(clanName, syncInfo, clanInfo)
	if syncInfo.Members then
		self:UpdateClientMembers(clanName, clanInfo.Members)
	end

	if syncInfo.JoinRequests then
		self:UpdateClientJoinRequests(clanName, clanInfo.JoinRequests)
	end

	if syncInfo.ClanEdit then
		self:UpdateClientEditInfo(clanName, clanInfo)
	end

	if syncInfo.Invites then
		self:UpdateClientInvites(clanName, clanInfo.Invites)
	end

	if syncInfo.Tasks then
		self:UpdateClientTasks(clanName, clanInfo.Tasks)
	end

	if syncInfo.LastTaskResets then
		self:UpdateClientTaskResets(clanName, clanInfo.LastTaskResets)
	end

	if syncInfo.Level then
		self:UpdateClientLevel(clanName, clanInfo.Level)
	end

	local memberAddedUserId = syncInfo.MemberAdded
	--print(memberAddedUserId, "MEMBER ADDED USERID")
	if memberAddedUserId then
		local playerInServer = Players:GetPlayerByUserId(memberAddedUserId)
		if playerInServer then
			if not self:GetClanInProfile(playerInServer) then
				self:SetClanInProfile(playerInServer, clanName)
			end
			self:UpdateClientMemberAdded(playerInServer)
		end
	end

	local memberRemovedUserId = syncInfo.MemberRemoved
	if memberRemovedUserId then
		local playerInServer = Players:GetPlayerByUserId(memberRemovedUserId)
		if playerInServer then
			self:UpdateClientMemberRemoved(playerInServer, syncInfo.Left, clanName)
			if self:GetClanInProfile(playerInServer) then
				self:SetClanInProfile(playerInServer, nil)
			end
		end
	end
end

function ClanService:PromoteMember(player, userId)
	local clanName = self:GetClanInProfile(player)
	if not clanName then
		return
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	local clanInfo = cachedClan.Info
	local memberInfo = clanInfo.Members[userId]
	if not memberInfo then
		return
	end

	local initiatorMemberInfo = clanInfo.Members[player.UserId]
	if not initiatorMemberInfo then
		return
	end

	local initRank = initiatorMemberInfo.Rank
	local rank = memberInfo.Rank

	if initRank == 1 then
		return
	end

	if initRank == 2 and rank >= 2 then
		return
	end

	if rank == 1 then
		self:SetMemberRank(clanName, userId, 2)
	elseif rank == 2 then
		self:SetMemberRank(clanName, userId, 3)
		self:SetMemberRank(clanName, player.UserId, 2)
	end
end

function ClanService:DemoteMember(player, userId)
	local clanName = self:GetClanInProfile(player)
	if not clanName then
		return
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	local clanInfo = cachedClan.Info
	local memberInfo = clanInfo.Members[userId]
	if not memberInfo then
		return
	end

	local initiatorMemberInfo = clanInfo.Members[player.UserId]
	if not initiatorMemberInfo then
		return
	end

	local initRank = initiatorMemberInfo.Rank
	local rank = memberInfo.Rank

	if initRank == 1 then
		return
	end

	if initRank == 2 and rank >= 2 then
		return
	end

	if rank == 1 then
		self:RemoveUserFromClan(clanName, userId)
	elseif rank == 2 then
		self:SetMemberRank(clanName, userId, 1)
	end
end

function ClanService:SetMemberRank(clanName, userId, rank)
	local cachedClan = self:GetCachedClan(clanName)

	local clanInfo = cachedClan.Info
	local memberInfo = clanInfo.Members[userId]

	cachedClan:SetMember(
		userId,
		{
			PetPower = memberInfo.PetPower or 0,
			Rebirth = memberInfo.Rebirth or 1,
			Rank = rank or memberInfo.Rank,
			Country = memberInfo.Country,
		}
	)

	self:AddClanToSync(cachedClan, {
		Members = true,
	})
end

function ClanService:ClanEdit(player, editiedData)
	local clanName = self:GetClanInProfile(player)
	if not clanName then
		return
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	local canUseDescription, err = checkString(player.UserId, editiedData.Description, 200)
	if not canUseDescription then
		return false, "Clan description " .. err
	end

	cachedClan:SetDescription(editiedData.Description)
	cachedClan:SetPrivacy(editiedData.Privacy)

	local emblemColor = editiedData.EmblemColor

	cachedClan:SetEmblem({
		color = {
			emblemColor.R,
			emblemColor.G,
			emblemColor.B
		},
		image = editiedData.EmblemImage,
		decal = editiedData.EmblemDecal,
	})

	self:AddClanToSync(cachedClan, {
		ClanEdit = true
	})

	return true
end

function ClanService:PublishAsync(topic, message)
	return Promise.retry(function()
		return Promise.new(function(resolve, reject)
			local success, err = pcall(function()
				MessagingService:PublishAsync(topic, message)
			end)
			
			if not success then
				warn(err)
				reject(err)
			end
			
			resolve()
		end)
	end, 10)
end

function ClanService:DenyJoinRequest(player, userId)
	local clanName = self:GetClanInProfile(player)
	if not clanName then
		return
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	-- Remove Request if they are already in a clan
	cachedClan:RemoveJoinRequest(userId)

	self:AddClanToSync(cachedClan, {
		JoinRequests = true,
	})

	-- If they aren't then add them and set join data
	local joinData = self:GetOutboundClanJoinData(userId)
	if joinData.lastClanAccepted then
		return
	end

	joinData.requests = {}
	
	ClanService:PublishAsync("JoinData", {
		Data = joinData,
		User = userId,
		SetOn = SERVER_ID,
	})

	self:SetJoinDataAsync(userId, joinData)
end

function ClanService:AcceptJoinRequest(player, userId)
	local clanName = self:GetClanInProfile(player)
	if not clanName then
		return
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	-- Remove Request if they are already in a clan
	cachedClan:RemoveJoinRequest(userId)

	self:AddClanToSync(cachedClan, {
		JoinRequests = true,
	})

	-- If they aren't then add them and set join data
	local joinData = self:GetOutboundClanJoinData(userId)
	if not joinData then
		return
	end
	
	print(joinData.lastClanAccepted, "LAST CLAN ACCEPTED")
	
	if joinData.lastClanAccepted then
		return
	end
	
	print(userId, type(userId))
	
	self:AddUserToClan(clanName, userId)
end

function ClanService:JoinClanRequest(player, clanName)
	if self:GetClanInProfile(player) then
		return false
	end

	if not clanName then
		return
	end

	local userId = player.UserId

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	if cachedClan.Info.Invites[player.UserId] then
		self:AcceptInvite(player, clanName)
		return true, "Joined"
	end

	if cachedClan.Info.IsPublic then
		self:AddUserToClan(clanName, userId, 1)
		return true, "Joined"
	end

	-- Setting join data on outbound (Per Client)
	local joinData = self:GetOutboundClanJoinData(userId)
	if not joinData then
		return
	end
	
	local requests = joinData.requests

	if requests[clanName] then
		return false, "Already sent to this clan"
	end

	requests[clanName] = true

	self:SetJoinDataAsync(userId, joinData)

	cachedClan:AddJoinRequest(userId)

	self:AddClanToSync(cachedClan, {
		JoinRequests = true,
	})

	return true, "Join request sent!"
end

function ClanService:SetJoinDataAsync(userId, newData)
	task.defer(CustomWriteQueueAsync, function()
		SafeDataStore.pushUpdate(outboundJoinDataDataStore, userId, function()
			return newData
		end)
	end, "JoinData", userId)
end

function ClanService:GetOutboundClanJoinData(userId)
	local playerJoinData = cachedJoinData[userId]

	if not playerJoinData then
		local newPlayerJoinData = {}

		local isSuccess = SafeDataStore.getData(outboundJoinDataDataStore, userId):andThen(function(joinDataFound)
			joinDataFound = joinDataFound or DeepCopyTable(JOIN_DATA_TEMP)
			newPlayerJoinData = joinDataFound
		end):await()

		if not isSuccess then
			warn("Failed to get join data for " .. userId)
			return
		end

		playerJoinData = newPlayerJoinData
		cachedJoinData[userId] = playerJoinData
	end

	return playerJoinData
end

function ClanService:LeaveClan(player)
	local profile, replica = DataService:GetUserData(player)
	local clanName = profile.clan

	local cachedClan = self:GetCachedClan(clanName)
	if not cachedClan then
		return
	end

	self:RemoveUserFromClan(clanName, player.UserId, true)

	return true
end

function ClanService:AcceptInvite(player, clanName)
	self:AddUserToClan(clanName, player.UserId, 1)

	return true
end

function ClanService:RejectClanInvite(player, clanName)
	self:RemoveInvite(player.UserId, clanName)

	return true
end

function ClanService:RemoveInvite(userId, clanName)
	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	cachedClan:RemoveInvite(userId)

	self:AddClanToSync(cachedClan, {
		Invites = true,
	})

	cachedInvites[userId][clanName] = nil

	local playerInServer = Players:GetPlayerByUserId(userId)
	if playerInServer then
		self.Client.InvitesChanged:Fire(playerInServer, cachedInvites[userId])
	end

	task.defer(CustomWriteQueueAsync, function()
		SafeDataStore.pushUpdate(inboundInvitesDataStore, userId, function(oldData)
			oldData = oldData or {}
			oldData[clanName] = nil
			return oldData
		end)
	end, "Invites", userId)
end

function ClanService:ClearInvites(userId)
	local cachedInvitesPlayer = self:GetInvites(userId)
	if not cachedInvites then
		return
	end
	
	print(next(cachedInvitesPlayer), "Cached invites table")

	for clanName, _ in cachedInvitesPlayer do
		print(clanName, "Cached Invite")

		local cachedClan = self:WaitForCachedClan(clanName)
		if not cachedClan then
			continue
		end

		cachedClan:RemoveInvite(userId)

		self:AddClanToSync(cachedClan, {
			Invites = true,
		})
	end

	cachedInvites[userId] = {}

	local playerIsInServer = Players:GetPlayerByUserId(userId)
	if playerIsInServer then
		self.Client.InvitesChanged:Fire(playerIsInServer, cachedInvites[userId])
	end

	task.defer(CustomWriteQueueAsync, function()
		SafeDataStore.pushUpdate(inboundInvitesDataStore, userId, function(oldData)
			return {}
		end)
	end, "Invites", userId)
end

function ClanService:SendInvite(playerSending, playerRequestedUsername)
	local isSuccess, userId = pcall(function()
		return Players:GetUserIdFromNameAsync(playerRequestedUsername)
	end)

	local success, userInfos = pcall(function()
		return UserService:GetUserInfosByUserIdsAsync({userId})
	end)

	if not success then
		return false, "Can't find player with username"
	end

	local joinData = self:GetOutboundClanJoinData(userId)
	if not joinData then
		return
	end

	if not isSuccess then
		return false, "Can't find player with username"
	end

	local profile, replica = DataService:GetUserData(playerSending)
	local clanName = profile.clan

	if not clanName then
		return false, "You are not in a clan"
	end

	local cachedInvitesPlayer = self:GetInvites(userId)
	if cachedInvitesPlayer[clanName] then
		self:RemoveInvite(userId, clanName)
		return true
	end

	if joinData.lastClanAccepted then
		return false, "Player has already joined a clan"
	end
	
	ClanService:PublishAsync("Invites", {
		UserInvited = userId,
		Clan = clanName,
		SetOn = SERVER_ID,
	})
	
	cachedInvitesPlayer[clanName] = true

	local playerInServer = Players:GetPlayerByUserId(userId)
	if playerInServer then
		self.Client.InvitesChanged:Fire(playerInServer, cachedInvitesPlayer)
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end
	
	cachedClan:AddInvite(userId)

	self:AddClanToSync(cachedClan, {
		Invites = true,
	})
	
	task.defer(CustomWriteQueueAsync, function()
		SafeDataStore.pushUpdate(inboundInvitesDataStore, userId, function(oldData)
			oldData = oldData or {}
			oldData[clanName] = true
			return oldData
		end)
	end, "Invites", userId)

	return true
end

function ClanService:GetInvites(userId)
	local playerInvites = cachedInvites[userId]

	if not playerInvites then
		local invites = {}

		local isSuccess = SafeDataStore.getData(inboundInvitesDataStore, userId):andThen(function(invitesFound)
			invitesFound = invitesFound or {}
			invites = invitesFound
		end):await()

		if not isSuccess then
			warn("Failed to get invites for " .. userId)
			return
		end

		playerInvites = invites
	end

	cachedInvites[userId] = playerInvites

	return playerInvites
end

function ClanService:GetClanInfo(clanName)
	if not clanName then
		return
	end

	local cachedClan = self:WaitForCachedClan(clanName)
	if not cachedClan then
		return
	end

	return cachedClan.Info 
end

--local start = os.time()

function ClanService:WaitForCachedClan(clanName, overideDestroyedCheck)
	--[[if os.time() - start < 5 then
		task.wait(5)
	end]]

	local cachedClan = self:GetCachedClan(clanName)
	local wasCreated = false
	if not cachedClan then
		wasCreated = true
		cachedClan = self:CreateCachedClan(clanName)
		cachedClan:Refresh()
	end

	local clanInfo = cachedClan.Info
	local members = 0
	for _, _ in clanInfo.Members do
		members += 1
	end

	if (clanInfo.IsDestroyed or members == 0) and not overideDestroyedCheck then
		print("STOPPED ", clanName, clanInfo)
		return false
	end
	
	if wasCreated then
		for userId, _ in clanInfo.Members do
			local playerInServer = Players:GetPlayerByUserId(userId)
			if not playerInServer or self:GetClanInProfile(playerInServer) then
				continue
			end
			
			self:SetClanInProfile(playerInServer, clanName)
		end
		
		task.defer(CustomWriteQueueAsync, function()
			SafeMemoryStore.addKey(recommendedClans, clanName)
		end, "Recommended", clanName)
	end

	return cachedClan
end

local function serializeClanData(dataSending)
	local newBitbuf = BitBuffer.new(1024)

	local function writeString(s)
		newBitbuf:WriteByte(#s)
		newBitbuf:WriteBytes(s)
	end

	writeString(dataSending.SetOn)
	writeString(dataSending.Name)

	local clanInfo = dataSending.Info

	writeString(clanInfo.Tag)
	writeString(clanInfo.Description)

	local function getDictionaryCount(table)
		local count = 0
		for _, _ in table do
			count += 1
		end
		return count
	end

	for key, value in clanInfo do
		--print(key, "KEY FOUND CLAN INFO")
	end

	--print(dataSending.Changed, "CHANGED KEY")

	local members = clanInfo.Members

	newBitbuf:WriteByte(getDictionaryCount(members))

	for userId, memberInfo in members do
		newBitbuf:WriteFloat(64, userId)
		writeString(memberInfo.Country)

		newBitbuf:WriteFloat(64, memberInfo.PetPower)
		newBitbuf:WriteByte(memberInfo.Rank)
		newBitbuf:WriteFloat(64, memberInfo.Rebirth)
	end

	local joinRequests = clanInfo.JoinRequests

	newBitbuf:WriteFloat(64, getDictionaryCount(joinRequests))

	for userId, _ in joinRequests do
		newBitbuf:WriteFloat(64, userId)
	end

	local invites = clanInfo.Invites

	newBitbuf:WriteFloat(64, getDictionaryCount(invites))

	for userId, _ in invites do
		newBitbuf:WriteFloat(64, userId)
	end

	newBitbuf:WriteBool(clanInfo.IsPublic)
	newBitbuf:WriteBool(clanInfo.IsDestroyed)

	local levelData = clanInfo.Level

	newBitbuf:WriteByte(levelData.Level)
	newBitbuf:WriteFloat(64, levelData.Exp)

	local lastTaskResets = clanInfo.LastTaskResets

	newBitbuf:WriteFloat(64, lastTaskResets.Daily)	
	newBitbuf:WriteFloat(64, lastTaskResets.Weekly)	

	local tasks = clanInfo.Tasks

	local dailyTasks = tasks.Daily
	local weeklyTasks = tasks.Weekly

	newBitbuf:WriteByte(getDictionaryCount(dailyTasks))
	newBitbuf:WriteByte(getDictionaryCount(weeklyTasks))

	for key, task in dailyTasks do
		writeString(key)

		newBitbuf:WriteFloat(64, task.Key)
		newBitbuf:WriteBool(task.Completed)
		newBitbuf:WriteFloat(64, task.Current)
	end

	for key, task in weeklyTasks do
		writeString(key)

		newBitbuf:WriteFloat(64, task.Key)
		newBitbuf:WriteBool(task.Completed)
		newBitbuf:WriteFloat(64, task.Current)
	end

	local emblemData = clanInfo.Emblem 

	local color3 = emblemData.color

	newBitbuf:WriteByte(color3[1])
	newBitbuf:WriteByte(color3[2])
	newBitbuf:WriteByte(color3[3])

	writeString(emblemData.image)
	writeString(emblemData.decal)

	local changed = dataSending.Changed

	newBitbuf:WriteByte(getDictionaryCount(changed))
	
	for changeType, changeValue in changed do
		writeString(changeType)
		
		local hasFloat = changeType == "MemberAdded" or changeType == "MemberRemoved"
		newBitbuf:WriteFloat(64, if hasFloat then changeValue else 0)
		newBitbuf:WriteBool(not hasFloat)
	end

	return newBitbuf:String()
end

--[[local newTaskTemplate = {
						Key = chosenTaskKey,
						Completed = false,
						Current = 0,
					}
]]

--[[
String serverSenderJobId
String clanName
OptionalString clanTag
UInt8 numberOfMembers
double numberOfJoinRequests
double numberOfInvites
Boolean isPublic
Boolean isDestroyed
UInt8 level
double exp
double lastDailyTaskReset
double lastWeeklyTaskReset
UInt8 numberOfDailyTasks
UInt8 numberOfWeeklyTasks
Color3 emblemColor
OptionalString emblemPresetNumberOrImageId

for each member:
    String country
    double petPower
    UInt8 rank
    double rebirth
    
for each task:
	double Key
	Boolean Completed
	double Current
]]

function ClanService:PublishToMessagingService(clanName, changed)
	local cachedClan = self:GetCachedClan(clanName)
	if not cachedClan then
		return
	end

	local clanInfo = cachedClan.Info

	local changedInfoKeys = {}
	for key, _ in changed do
	--	print(key, "CHANGED KEY")
		changedInfoKeys[key] = clanInfo[key]
	end

	local dataSending = {
		Info = clanInfo,
		Name = clanName,
		Changed = changed,
		SetOn = SERVER_ID,
	}

	--[[if key == "Members" then
		print("Do binary stuff according to members")
	elseif key == "Tasks" then

	end]]

	local bitbufString = serializeClanData(dataSending)
	
	ClanService:PublishAsync("Clans", Ascii85.encode(bitbufString))
end

-- Ignore parameter exists to stop 2 updates from going to the same client. The create clan update and the update members both go to the client that was added if in game.
function ClanService:UpdateClientMembers(clanName, members, ignorePlayer)
	self.Client.MembersChanged:FireFor(
		self:GetMembersOfClanInServer(clanName, ignorePlayer),
		members
	)
end

function ClanService:UpdateClientMemberAdded(player)
	self.Client.PlayerAddedToClan:Fire(player)
end

function ClanService:UpdateClientMemberRemoved(player, hasLeft, clanName)
	self.Client.PlayerRemovedFromClan:Fire(player, if hasLeft then "You have left the team." else "Kicked from team.", clanName)
end

function ClanService:UpdateClientInvites(clanName, invites)
	self.Client.ClanInvitesChanged:FireFor(
		self:GetMembersOfClanInServer(clanName),
		invites
	)
end

function ClanService:UpdateClientJoinRequests(clanName, joinRequests)
	self.Client.JoinRequestsChanged:FireFor(
		self:GetMembersOfClanInServer(clanName),
		joinRequests
	)
end

function ClanService:UpdateClientEditInfo(clanName, clanInfo)
	self.Client.EditInfoChanged:FireFor(
		self:GetMembersOfClanInServer(clanName),
		clanInfo
	)

	self.Client.InfoChanged:FireAll(clanName)
end

function ClanService:UpdateClientTasks(clanName, tasks)
	self.Client.TasksChanged:FireFor(
		self:GetMembersOfClanInServer(clanName),
		tasks
	)
end

function ClanService:UpdateClientTaskResets(clanName, taskResets)
	self.Client.TaskResetsChanged:FireFor(
		self:GetMembersOfClanInServer(clanName),
		taskResets
	)
end

function ClanService:UpdateClientLevel(clanName, levelData)
	self.Client.LevelChanged:FireFor(
		self:GetMembersOfClanInServer(clanName),
		levelData
	)
end

function ClanService:GetMembersOfClanInServer(clanName, ignore)
	local clanInfo = self:GetClanInfo(clanName)
	if not clanInfo then
		return {}
	end

	local membersInServer = {}
	for _, player in Players:GetPlayers() do
		local userId = player.UserId

		if not clanInfo.Members[player.UserId] or userId == ignore then
			continue
		end

		table.insert(membersInServer, player)
	end

	return membersInServer
end

function ClanService:GetCachedClan(clanName)
	return cachedClans[clanName]
end

function ClanService:CreateNewClan(clanName, isPublic, clanTag, clanDescription, clanColor, clanEmblem, clanDecal)
	local newClan = self:GetCachedClan() or self:CreateCachedClan(clanName)

	newClan:SetDescription(clanDescription or "")
	newClan:SetTag(clanTag or "")
	newClan:SetPrivacy(isPublic)
	newClan:SetEmblem({
		color = {
			clanColor.R,
			clanColor.G,
			clanColor.B
		},
		image = clanEmblem,
		decal = clanDecal,
	})
	newClan:Init()

	self:AddClanToSync(newClan, {
		Destroyed = true,
		ClanEdit = true
	})
end

function ClanService:CreateCachedClan(clanName)
	local newClan = ClanClass.new(clanName)
	cachedClans[clanName] = newClan
	return newClan
end

function ClanService:AddClanToSync(newClan, syncInfo)
	--print(newClan, " SYNCED")

	local info = newClan.Info
	local clanName = info.Name

	if clansToSync[clanName] then
		clansToSync[clanName] = TableUtil.Reconcile(clansToSync[clanName], syncInfo)
		return
	end

	clansToSync[clanName] = syncInfo
end

function ClanService:CreateClan(player, clanName, isPublic, clanTag, clanDescription, clanColor3, clanEmblem, clanDecal)
	local rawClanInfo = ClanClass.GetClanInfoRaw(clanName)
	if rawClanInfo and not rawClanInfo.IsDestroyed then
		return false, "Clan with name already exists!"
	end

	local userId = player.UserId

	local canUseName, err = checkString(userId, clanName, 20)
	if not canUseName then
		return false, "Clan name " .. (err or " failed to check")
	end

	local canUseTag, err = checkString(userId, clanTag, 5)
	if not canUseTag then
		return false, "Clan tag " .. (err or " failed to check")
	end	

	local canUseDescription, err = checkString(userId, clanDescription, 200)
	if not canUseDescription then
		return false, "Clan description " .. (err or " failed to check")
	end

	if typeof(clanColor3) ~= "Color3" then
		return false, "Clan Color Incorrect Datatype"
	end

	local profile = DataService:GetUserData(player)
	if profile.gems < CLAN_PRICE then
		return false, "Not enough gems!"
	end

	clanEmblem = clanEmblem or ""
	clanDecal = clanDecal or ""

	CurrencyService:GiveGems(player, -CLAN_PRICE)

	local cachedClan = self:CreateNewClan(clanName, isPublic, clanTag, clanDescription, clanColor3, clanEmblem, clanDecal)

	self:AddUserToClan(clanName, player.UserId, 3)

	return true
end

function ClanService:AddUserToClan(clanName, userId, rank)
	local cachedClan = self:GetCachedClan(clanName)
	if not cachedClan then
		return
	end
	
	local joinData = self:GetOutboundClanJoinData(userId)
	if not joinData then
		return
	end

	joinData.requests = {}
	joinData.lastClanAccepted = clanName
	
	ClanService:PublishAsync("JoinData", {
		Data = joinData,
		User = userId,
		SetOn = SERVER_ID,
	})
	
	self:SetJoinDataAsync(userId, joinData)
	self:ClearInvites(userId)
	
	local hasALeader = false
	for _, memberInfo in cachedClan.Info.Members do
		if memberInfo.Rank ~= 3 then
			continue
		end
		
		hasALeader = true
	end
	
	if not hasALeader then
		rank = 3
	end
	
	cachedClan:SetMember(
		userId,
		{
			PetPower = joinData.lastPetPower or 0,
			Rebirth = joinData.lastRebirth or 1,
			Rank = rank or 1,
			Country = joinData.country or "SS",
		}
	)

	cachedClan:RemoveJoinRequest(userId)

	self:AddClanToSync(cachedClan, {
		Members = true,
		MemberAdded = userId,
	})

	local playerInServer = Players:GetPlayerByUserId(userId)
	if playerInServer then
		self:SetClanInProfile(playerInServer, clanName)
	end
end

function ClanService:RemoveUserFromClan(clanName, userId, hasLeft)
	local cachedClan = self:GetCachedClan(clanName)
	local clanInfo = cachedClan.Info
	local clanMembers = clanInfo.Members
	
	local memberInfo = clanMembers[userId]
	if not memberInfo then
		warn(userId .. " was not found in clan members list")
		return
	end
	
	local oldestUser, oldestTime = nil, 0
	local isOwner = memberInfo.Rank == 3

	-- Since this member was the leader, rank oldest member to the leader
	if isOwner then
		for userIdFound, userInfo in clanMembers do
			if userIdFound == userId then
				continue
			end

			local timeInClan = os.time() - (userInfo.JoinTime or 0)
			if timeInClan > oldestTime then
				oldestTime = timeInClan
				oldestUser = userIdFound
			end
		end

		if oldestUser then
			local newMemberInfo = clanMembers[oldestUser]
			newMemberInfo.Rank = 3

			cachedClan:SetMember(
				oldestUser,
				newMemberInfo
			)
		end
	end

	cachedClan:SetMember(
		userId,
		nil
	)

	local memberCount = 0
	for _, _ in clanInfo.Members do
		memberCount += 1
	end

	if memberCount == 0 then
		cachedClan:Destroy()
		self:AddClanToSync(cachedClan, {
			Destroyed = true,
		})
	end

	self:AddClanToSync(cachedClan, {
		Members = true,
		MemberRemoved = userId,
		Left = hasLeft,
	})

	local playerInServer = Players:GetPlayerByUserId(userId)
	if playerInServer then
		self:SetClanInProfile(playerInServer, nil)
	end

	local joinData = self:GetOutboundClanJoinData(userId)
	if not joinData then
		return
	end

	joinData.lastClanAccepted = nil
	joinData.requests = {}
	
	ClanService:PublishAsync("JoinData", {
		Data = joinData,
		User = userId,
		SetOn = SERVER_ID,
	})
	
	self:SetJoinDataAsync(userId, joinData)
end

function ClanService:SetClanInProfile(player, clan)
	local profile, replica = DataService:GetUserData(player)
	local previousClan = profile.clan
	profile.clan = clan
	replica:SetValue("clan", clan)
	ChatService:UpdateTags(player.Name)
	if clan then
		ChatService:JoinChannel(player.Name, clan)
		self.updateConnection:Fire(clan)
	else
		if previousClan then
			ChatService:LeaveChannel(player.Name, previousClan)
			self.updateConnection:Fire(nil)
		end
	end
end

function ClanService:GetClanInProfile(player)
	local profile = DataService:GetUserData(player)
	return profile.clan
end

return ClanService
