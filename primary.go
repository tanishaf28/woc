package main

import (
	"woc/config"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

// DEPRECATED: Use establishServerConnections() from conns.go instead
// This function is kept for backward compatibility but is no longer used
// All servers now use the unified establishServerConnections() function
func establishRPCs() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	id := config.ServerID
	ip := config.ServerIP
	portOfRPCListener := config.ServerRPCListenerPort

	for i := 0; i < numOfServers; i++ {
		if i == myServerID {
			continue
		}

		serverID, err := strconv.Atoi(serverConfig[i][id])
		if err != nil {
			log.Errorf("%v", err)
			continue
		}

		newServer := &ServerDock{
			serverID:   serverID,
			addr:       serverConfig[i][ip] + ":" + serverConfig[i][portOfRPCListener],
			txClient:   nil,
			jobQMu:     sync.RWMutex{},
			jobQ:       map[prioClock]chan struct{}{},
		}
		
		log.Infof("i: %d | newServer.addr: %v", i, newServer.addr)
		var txClient *rpc.Client
		maxRetries := 10
		retryDelay := 1 * time.Second
		
		for attempt := 1; attempt <= maxRetries; attempt++ {
			txClient, err = rpc.Dial("tcp", newServer.addr)
			if err == nil {
				log.Infof(" txClient connected to server %d on attempt %d", serverID, attempt)
				break
			}
			
			if attempt < maxRetries {
				log.Warnf("txClient connection attempt %d/%d to server %d failed: %v. Retrying in %v...", 
					attempt, maxRetries, serverID, err, retryDelay)
				time.Sleep(retryDelay)
			} else {
				log.Errorf("txClient failed to connect to server %d after %d attempts: %v", 
					serverID, maxRetries, err)
			}
		}
		
		if err != nil {
			log.Errorf("Skipping server %d - could not establish txClient connection", serverID)
			continue // Skip this server, try next one
		}
		newServer.txClient = txClient
		
		conns.Lock()
		conns.m[serverID] = newServer
		conns.Unlock()
		log.Infof(" Established RPC connections to server %d at %s", serverID, newServer.addr)
	}
	conns.RLock()
    connectedServers := len(conns.m)
    conns.RUnlock()
    
    log.Infof("RPC establishment complete: connected to %d/%d servers", connectedServers, numOfServers-1)

}