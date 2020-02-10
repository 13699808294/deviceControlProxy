import datetime
import time
import uuid
from json import JSONDecodeError
from urllib.parse import urlparse

import tornado.websocket
from tornado import gen
import json

from setting.setting import MY_SQL_SERVER_HOST, DATABASES, ENVIRONMENT
from utils.MysqlClient import MysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps
from tornado.websocket import WebSocketClosedError

class WebsocketClient(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        parsed_origin = urlparse(origin)
        origin = parsed_origin.netloc
        origin = origin.lower()

        host = self.request.headers.get("Host")
        # Check to see that origin matches host directly, including ports
        self.host = host
        origin = origin.split(':')[0]
        host = host.split(':')[0]
        # if ENVIRONMENT == 'build':
        #     return origin == host
        # else:
        return True


    def initialize(self, server):
        self.server = server        #WebsocketServer对象
        self.ioloop = server.ioloop #
        self.sync_delay_dict = server.sync_delay_dict
        self.host = None
        self.login = None
        self.connection_token = None
        self.last_connection_token = []
        self.user_type = None
        self.user_name = None
        self.user_id = None
        self.lastHeartbeat = time.time()
        self.room_list = []
        self.use_room_list = []
        self.isAdmin = True
        self.lastRxTime = 0
        self.topics = ['*']
        self.query_task_dict = {}
        self.last_control_dict = {}
        self.logined = 0

    @gen.coroutine
    def open(self):
        self.server.clientObjectSet.add(self)
        yield logClient.tornadoDebugLog('websocket连接接入,当前连接数量:{}'.format(len(self.server.clientObjectSet)))

    @gen.coroutine
    def sendToWebsocketClient(self,msg):
        # print(msg)
        try:
            self.write_message(msg)
        except Exception as e:
            yield logClient.tornadoErrorLog(e)
            return

    @gen.coroutine
    def on_message(self, data):
        '''
        1>接受前端的信息,2>发送给websocketService,3>接受websocketService的消息,4>分发给前端
        '''
        try:
            msg = json.loads(data)
        except JSONDecodeError as e:
            try:
                msg = eval(data)
            except:
                return

        if not isinstance(msg,dict):
            return
        yield logClient.tornadoDebugLog('1---客户端信息:{}'.format(msg))

        # 获取操作类型
        request_type = msg.get('type')
        # 1:获取连接token,直接生产一个token返回给前端
        if request_type == 'login':
            if self.logined == 1:
                self.last_connection_token.append(self.connection_token)
            self.connection_token = uuid.uuid1().urn.split(':')[2]
            msg['connection_token'] = self.connection_token
            if self.host == None:
                # self.close()
                # return
                self.host = '127.0.0.1'
            msg['host'] = self.host
            self.server.sendToService(json_dumps(msg))
            return

        # 2:接收心跳
        elif request_type == 'heartbeat':
            self.lastHeartbeat = time.time()
            return

        # 3:否则转发websocketService
        else:
            connection_token = msg.get('connection_token',)
            # 2.1:connection_token验证
            if connection_token != self.connection_token:
                msg = {
                    'ret': 1,
                    'type':request_type,
                    'errmsg': 'connection_token error'
                }
                self.sendToWebsocketClient(json_dumps(msg))
                return
            # 2.2:记录控制任务
            if request_type == 'control':
                guid = msg.get('guid')
                meeting_room_guid = msg.get('meeting_room_guid')
                event_type = msg.get('event_type')
                if event_type == 'click' or event_type == 'on' or event_type == 'off' or event_type == 'pulse' or event_type == 'toggle':
                    #查阅guid是否在上次控制缓存中
                    if guid in self.last_control_dict.keys():
                        if event_type == 'click':
                            if msg['status'] == 'on':
                                msg['status'] = 'off'
                            elif msg['status'] == 'off':
                                msg['status'] = 'on'

                        yield self.sendToWebsocketClient(json_dumps(msg))
                        return
                    else:
                        self.last_control_dict[guid] = time.time()
                        self.ioloop.add_timeout(self.ioloop.time()+0.6,self.deleteControlRecord,guid)

                delay_time = self.sync_delay_dict.get(guid, 3)
                new_later_task = self.ioloop.add_timeout(self.ioloop.time()+delay_time, self.query_status, guid,meeting_room_guid)
                # 尝试获取相同guid已有查询任务
                last_later_task = self.query_task_dict.get(guid)
                if last_later_task != None:
                    self.ioloop.remove_timeout(last_later_task)
                self.query_task_dict[guid] = new_later_task
            self.server.sendToService(data)
            return

    @gen.coroutine
    def on_close(self):
        #从server的websocket对象集合中移除自己
        self.server.clientObjectSet.discard(self)
        yield logClient.tornadoInfoLog('用户 {} 退出登录,当前连接数量:{}'.format(self.user_name,len(self.server.clientObjectSet)))
        msg = {
            'type':'logout',
            'connection_token':self.connection_token
        }
        self.server.sendToService(json_dumps(msg))
        self = None

    def deleteControlRecord(self,guid):
        if guid in self.last_control_dict.keys():
            del self.last_control_dict[guid]

    def query_status(self,guid,meeting_room_guid):
        data = {
            'type': 'query',
            'guid':guid,
            'meeting_room_guid':meeting_room_guid,
            'connection_token': self.connection_token
        }
        # print('执行查询{}'.format(guid))
        self.server.sendToService(json_dumps(data))
        try:
            del self.query_task_dict[guid]
        except:
            pass
