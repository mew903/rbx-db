# RbxDb

## A simple solution for Roblox DataStores
RbxDb is a simple, lightweight, queue-based DataStore wrapper for the Roblox engine.

## Features
- Automatic request throttling
- Optional data caching 

## Syntax
`RbxDb(DataStoreName : string, DataStoreScope : string)`

# RbxProfile
RbxProfile is an extension of RbxDb which intends to provide a simple and flexible solution for the creation and management of user profiles.

## Features
- Flexible data reconciliation
- Template-based profile construction
- Safe endpoints for server-client interactions

## Syntax
`RbxDb.Profile(Player : Instance, Template : dict)`

# RbxDb Console _(soon)_
RbxDb Console is a 3-in-1 interface for Roblox game developers to visualize their DataStore activity and provide solutions for common management tasks

## Features
- Verbose output panel for monitoring real-time server activity
- Terminal panel which includes built-in tools to automate DataStore management tasks
- Explorer which lists all RbxDb DataStores active in the current server, along with tools to view and manage their data

## Syntax
`RbxDb.EnableConsoleAccess(Player : Instance)`
