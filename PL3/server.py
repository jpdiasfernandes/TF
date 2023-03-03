#!/usr/bin/env python3

# Simple 'echo' workload in Python for Maelstrom
#./maelstrom test -w lin-kv --bin ~/Desktop/uni/4ano/2semestre/TF/guiao0/lin-kv.py --time-limit 5 --node-count 1 --concurrency 2

import logging
from ms import receiveAll, reply, send
from bisect import insort

logging.basicConfig(level=logging.DEBUG)

kv = {}

queue = []
# id -> ts
ts_servers = {}
# ts -> client
actions_client = {}

ts = 0

class Action:
    def __init__(self, action, key, value=None):
        self.key = key
        self.action = action
        if value != None:
            self.value = value

class Request:
    def __init__(self, timestamp, id, actionObj):
        self.timestamp = timestamp
        self.id = id
        self.action = actionObj

    def __lt__(self, req):
        if self.timestamp == req.timestamp:
            return self.id < req.id

        return self.timestamp < req.timestamp
    def __str__ (self):
        return '(T' + str(self.timestamp) + ':P' + self.id + ')'
    def __repr__(self):
        return self.__str__()


def updateTs(t=None, node=None):
    global ts
    ts += 1
    if t != None and node != None:
        ts = max(ts, t)
        ts_servers[node] = t
        logging.info('updated ts %d :: node %s :: node_ts %d :: msg_id %d' , ts, node, t, msg.body.msg_id)

    logging.info('updated ts %d :: msg_id %d' , ts, msg.body.msg_id)

def sendRelease(node):
    send(node_id, node, type='release', ts=ts, id=node_id)
    logging.info('sending release src %s :: dest %s :: (%d:%s)', node_id, node, ts, node_id)

def sendRequest(request, node):
    action = request.action.action
    actionObj = request.action
    if action == "write":
        send(node_id, node, type='request', ts=request.timestamp, id=request.id, action="write", key=actionObj.key, value=actionObj.value)
    elif action == "read":
        send(node_id, node, type='request', ts=request.timestamp, id=request.id, action="read", key=actionObj.key)

    logging.info('sending request src %s :: dest %s :: (%d:%s)', node_id, node, ts, node_id)

def getResource(key, value=None):
    if value != None:
        action = Action("write",key,value)
    else:
        action = Action("read", key)

    req = Request(ts, node_id, action)
    for node in node_ids:
        if node != node_id:
            sendRequest(req, node)

    insort(queue,req)

def sendRequestAck(msg,ts):
    reply(msg, type='req_ack', ts=ts)
    logging.info('sending ack src %s :: dest %s :: ts %d', node_id, msg.src , ts)


def removeRequests(node):
    for i,req in enumerate(queue):
        if req.id == node:
            logging.info('removing (%d:%s) request index %d', req.timestamp, req.id, i)
            queue.pop(i)
            break

def doAction(node):
    global queue
    logging.info('Entered doAction function')
    for req in queue:
        action = req.action
        if req.id == node and action.action == "write":
            logging.info('Writing kv[%s]=%s', action.key, action.value)
            kv[action.key] = action.value

def checkTs(t):
    my_req = Request(t, node_id, None)
    for i,tsp in ts_servers.items():
        req = Request(tsp,i, None)
        if req < my_req:
            return False

    return True

def allTs():
    # ts from the others servers plus own server is equal to all the servers in the system
    return len(list(ts_servers.keys())) + 1 == len(node_ids)

def tryExecRequest():
    if len(queue) > 0:
        id = queue[0].id
        t = queue[0].timestamp
        action = queue[0].action
        # Se for uma request minha e os outros servidores tiverem ts superiores
        # Fazer a ação
        if id == node_id and allTs() and checkTs(t):
            # Action means updating Ts
            updateTs()
            logging.info('Executing my request with action %s', action.action)
            client = actions_client[t]
            action_v = action.action
            key = action.key
            if action_v == 'read':
                if key not in kv.keys():
                    logging.info('Sending error key not found to client %s', client)
                    send(node_id, client, type='error', code=20, text='key not found')
                else:
                    logging.info('Sending read_ok client %s', client)
                    send(node_id, client, type='read_ok', value=kv[key])
            elif action_v == 'write':
                value = action.value
                logging.info('Writing kv[%s]=%s', action.key, action.value)
                kv[key] = value
                logging.info('Sending write_ok client %s', client)
                send(node_id, client, type='write_ok')

            removeRequests(node_id)
            #enviar mensagens de release
            for node in node_ids:
                if node != node_id:
                    sendRelease(node)






def put(key,value, ts, writer):
    kv[key] = {"value" : value, "timestamp" : ts, "writer" : writer}

def get(key):
    return kv[key]

for msg in receiveAll():
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)
        reply(msg, type='init_ok')

    elif msg.body.type == 'read':
        logging.info('reading on key %s :: msg_id %d', msg.body.key, msg.body.msg_id)
        # New event update ts
        updateTs()
        actions_client[ts] = msg.src
        getResource(msg.body.key)


    elif msg.body.type == 'write':
        logging.info('writing on key %s :: value %s :: msg_id %d ', msg.body.key, msg.body.value, msg.body.msg_id)
        # New event update ts
        updateTs()
        actions_client[ts] = msg.src
        getResource(msg.body.key, msg.body.value)

    elif msg.body.type == 'cas':
        logging.info('cas on key %s :: from %s :: to %s :: msg_id %d', msg.body.key, getattr(msg.body,'from'), msg.body.to, msg.body.msg_id)
        reply(msg, type= 'error', code=10, text="Not supported")

    elif msg.body.type == 'request':
        # New event update ts
        if msg.body.action == "write":
            action = Action("write", msg.body.key, msg.body.value)
        else:
            action = Action("read", msg.body.key)

        req = Request(msg.body.ts, msg.body.id, action)
        updateTs(t=req.timestamp, node=msg.src)
        insort(queue, req)
        sendRequestAck(msg,ts)

    elif msg.body.type == 'release':
        updateTs(t=msg.body.ts, node=msg.src)
        doAction(msg.src)
        removeRequests(msg.src)

    elif msg.body.type == 'req_ack':
        updateTs(t=msg.body.ts, node=msg.src)

    else:
        logging.warning('unknown message type %s', msg.body.type)

    tryExecRequest()

#req1 = Request(0,"node_1")
#req2 = Request(0, "node_2")
#insort(queue, req2)
#insort(queue, req1)
#print(queue)
