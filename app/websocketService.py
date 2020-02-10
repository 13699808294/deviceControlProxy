import asyncio
import json
import re
import time

import uvloop
from tornado import web, httpserver, gen
from tornado.ioloop import IOLoop, PeriodicCallback
from concurrent.futures import ThreadPoolExecutor

from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.websocket import websocket_connect

from app.WebsocketClient import WebsocketClient
from setting.setting import DEBUG, WS_HOST, ENVIRONMENT, DATABASES, STATUS_SWICH
from utils.MysqlClient import mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps


class WebsocketServer(object):
    executor = ThreadPoolExecutor(max_workers=10)
    def __init__(self,aioloop=None,ioloop=None):
        self.aioloop = aioloop
        self.ioloop = ioloop

        # websocket对象保持集合
        self.clientObjectSet = set()
        self.sync_channel_dict = {}
        self.sync_delay_dict = {}
        self.macro_back_dict = {}
        self.serviceObject = None
        self.mutex_group_list = []

        # 每 20 秒發送一次 ping
        PeriodicCallback(self.wsClientKeep_alive, 1000).start()
        self.heartDelayCount = 0

        self.ioloop.add_timeout(self.ioloop.time() + 15, self.checkClientConnectionStatus)
        self.ioloop.add_timeout(self.ioloop.time(), self.wsClientConnect)

        self.ioloop.add_timeout(self.ioloop.time(), self.getMacroSyncChannel)
        self.ioloop.add_timeout(self.ioloop.time(), self.getChannelSyncDelay)
        # self.ioloop.add_timeout(self.ioloop.time(), self.getMacroBackMessage)
        self.ioloop.add_timeout(self.ioloop.time(),self.getMutexChannel)
        # 最后 :todo:启动服务
        ssl_options = {
            'certfile': '/ssl_file/websocket.crt',
            'keyfile': '/ssl_file/websocket.key'
        }
        wsApp = web.Application([(r'/', WebsocketClient,{'server': self})])
        if ENVIRONMENT == 'local' or ENVIRONMENT == 'local_test':
            wsServer = httpserver.HTTPServer(wsApp)
        else:
            wsServer = httpserver.HTTPServer(wsApp, ssl_options=ssl_options)
        wsServer.listen(8010)
        logClient.debugLog('**********************启动wsServer××××××××××××××××××××')

    #------------------------------------------------------------------------------------------------------------------#
    # todo:检验所有连接对象超时
    @gen.coroutine
    def checkClientConnectionStatus(self):
        now_time = time.time()
        # print(len(self.clientObjectSet))
        for clientObject in self.clientObjectSet:
            if (now_time - clientObject.lastHeartbeat) >= 60:
                clientObject.close()
                self.clientObjectSet.discard(clientObject)
                del clientObject
                self.ioloop.add_timeout(self.ioloop.time()+1, self.checkClientConnectionStatus)
                break
        self.ioloop.add_timeout(self.ioloop.time() + 10, self.checkClientConnectionStatus)

    #todo:获取通道同步延时
    @gen.coroutine
    def getChannelSyncDelay(self):
        '''
        默认值为0,如果不为0表示,对该通道的同步延时做了要求,需要按照要求同步
        :return:
        '''
        for DATABASE in DATABASES:
            db = DATABASE['name']
            data = {
                'database': db,
                'fields': ['guid',
                           'sync_delay',
                           ],
                'eq': {
                    'is_delete': False,
                },
                'neq':{
                    'sync_delay':0
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_control_channel', data)
            if msg['ret'] != '0':
                yield logClient.tornadoErrorLog('数据库{},查询失败:{}'.format(db,'d_control_channel'))
                continue
            self.sync_delay_dict = {**self.sync_delay_dict,**{x['guid']: x['sync_delay'] for x in msg['msg']}}

    #todo:获取聚集同步信息
    @gen.coroutine
    def getMacroSyncChannel(self):
        '''
        控制聚集通道时,可以提前知道子通道的状态
        self.
        数据结构 {
            ‘聚集通道guid’:{
                '聚集通道的状态(on)':[
                    {
                        ‘sync_channel_guid’:'子通道guid'
                        ''sync_channel_status:‘子通道状态’
                    },{}
                ]
            }
        }
        '''
        for DATABASE in DATABASES:
            db = DATABASE['name']
            data = {
                'database': db,
                'fields': ['d_sync_channel.control_channel_guid_id',
                           'd_sync_channel.control_channel_status',
                           'd_sync_channel.sync_channel_guid_id',
                           'd_sync_channel.sync_channel_status',
                           'd_control_channel.meeting_room_guid_id',
                           ],
                'eq': {
                    'd_sync_channel.is_delete': False,
                    'd_sync_channel.sync_channel_guid_id':{'key':'d_control_channel.guid'}

                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_sync_channel,d_control_channel', data)
            if msg['ret'] != '0':
                yield logClient.tornadoErrorLog('数据库:{},查询失败:{}'.format(db,'d_macro_sync_channel'))
                continue
            macro_sync_channel_list = msg['msg']
            for macro_sync_channel in macro_sync_channel_list:
                control_channel_guid = macro_sync_channel['control_channel_guid_id']
                control_channel_status = macro_sync_channel['control_channel_status']
                sync_channel_guid = macro_sync_channel['sync_channel_guid_id']
                sync_channel_status = macro_sync_channel['sync_channel_status']
                meeting_room_guid = macro_sync_channel['meeting_room_guid_id']
                info = {
                            'sync_channel_guid': sync_channel_guid,
                            'sync_channel_status': sync_channel_status,
                            'meeting_room_guid':meeting_room_guid,
                        }
                if control_channel_guid not in self.sync_channel_dict.keys():
                    self.sync_channel_dict[control_channel_guid] = {
                        control_channel_status: [info]
                    }
                else:
                    if control_channel_status not in self.sync_channel_dict[control_channel_guid].keys():
                        self.sync_channel_dict[control_channel_guid][control_channel_status] = [info]
                    else:
                        self.sync_channel_dict[control_channel_guid][control_channel_status].append(info)
        else:
            pass

    #todo:获取互斥信息
    @gen.coroutine
    def getMutexChannel(self):
        mutex_group_guid_list = []
        for DATABASE in DATABASES:
            db = DATABASE['name']
            data = {
                'database':db,
                'fields':['guid'],
                'eq':{
                    'is_delete':False,
                }
            }
            msg = yield mysqlClient.tornadoSelectAll('d_mutex_group', data)
            if msg['ret'] != '0':
                yield logClient.tornadoErrorLog('数据库:{},查询失败:{}'.format(db,'d_mutex_group'))
                return
            mutex_group_guid_list += [x['guid'] for x in msg['msg']]

            for mutex_group_guid in mutex_group_guid_list:
                data = {
                    'database': db,
                    'fields': [
                        'd_mutex_channel.control_channel_guid_id',
                        'd_mutex_channel.positive_value',
                        'd_mutex_channel.negative_value',
                        'd_control_channel.meeting_room_guid_id',
                    ],
                    'eq': {
                        'd_mutex_channel.is_delete': False,
                        'd_control_channel.guid':{'key':'d_mutex_channel.control_channel_guid_id'},
                        'mutex_group_guid_id':mutex_group_guid
                    }
                }
                msg = yield mysqlClient.tornadoSelectAll('d_mutex_channel,d_control_channel', data)
                if msg['ret'] != '0':
                    yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_macro_back'))
                    return
                mutex_channel_info = msg['msg']
                info = {
                    'mutex_channel_info':{x['control_channel_guid_id']:{'positive_value':x['positive_value'],'negative_value':x['negative_value'],'meeting_room_guid':x['meeting_room_guid_id']} for x in mutex_channel_info},
                    'channel_guid_list':[x['control_channel_guid_id'] for x in mutex_channel_info]
                }
                self.mutex_group_list.append(info)

    #todo:获取聚集反馈信息
    @gen.coroutine
    def getMacroBackMessage(self):
        '''
        前端控制子通道时,可以提前得到聚集通道的状态
        self.macro_back_dict
        数据结构
        {
            '聚集通道guid'[
                [{
                    'channel_guid':'子通道guid',
                    'target_status':'目标状态',
                    'now_status':'当前状态'
                },
                {...}],
                [{
                    'channel_guid':'子通道guid',
                    'target_status':'目标状态',
                    'now_status':'当前状态'
                },
                {...}],
                ...
            ],
            ...
        }
        '''
        for DATABASE in DATABASES:
            db = DATABASE['name']
            data = {
                'database': db,
                'fields': ['macro_channel_guid_id',
                           'macro_message',
                           ],
                'eq': {
                    'is_delete': False,

                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_macro_back', data)
            if msg['ret'] != '0':
                yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_macro_back'))
                return
            macro_back_list = msg['msg']
            for macro_back_info in macro_back_list:
                macro_channel_guid = macro_back_info.get('macro_channel_guid_id')
                macro_message = macro_back_info.get('macro_message')
                #获取聚集通道所在会议室
                data = {
                    'database':db,
                    'fields':['meeting_room_guid_id'],
                    'eq':{
                        'is_delete':False,
                        'guid':macro_channel_guid,
                    }
                }
                msg = yield mysqlClient.tornadoSelectOne('d_control_channel', data)
                if msg['ret'] != '0':
                    yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_control_channel'))
                    continue
                channel_info = msg['msg']
                if channel_info == None:
                    continue

                self.macro_back_dict[macro_channel_guid] = []
                macro_message = macro_message.replace('(','')
                macro_message = macro_message.replace(')', '')
                meeting_room_guid = msg['msg'].get('meeting_room_guid_id')
                or_relation_list = macro_message.split(' or ')
                for or_relation in or_relation_list:
                    and_relation = or_relation.split(' and ')
                    and_ = []
                    for and_info in and_relation:
                        and_info_list = and_info.split(' == ')
                        r = re.findall('channel\[(.+?)\]\[(.+?)\]',and_info_list[0])
                        if r == []:
                            r = re.findall('level\[(.+?)\]\[(.+?)\]',and_info_list[0])
                        port = int(r[0][0])
                        channel = int(r[0][1])
                        try:
                            value = eval(and_info_list[1])
                        except:
                            value = and_info_list[1]
                        channel_guid = yield self.getChnnelGuid(db,meeting_room_guid,port,channel)
                        if channel_guid == False:
                            continue
                        info = {
                            'channel_guid':channel_guid,
                            'target_status':value,
                            'now_status':None
                        }
                        and_.append(info)
                    self.macro_back_dict[macro_channel_guid].append(and_)

    #todo:获取通道guid
    @gen.coroutine
    def getChnnelGuid(self,db,meeting_room_guid,port,channel):
        data = {
            'database': db,
            'fields': ['guid'],
            'eq': {
                'is_delete': False,
                'meeting_room_guid_id': meeting_room_guid,
                'port': port,
                'channel': channel,
            }
        }
        msg = yield mysqlClient.tornadoSelectOne('d_control_channel', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_control_channel'))
            return False
        channel_guid = msg['msg'].get('guid')
        if channel_guid == None:
            return False
        return channel_guid

    @gen.coroutine
    def wsClientConnect(self):
        self.serviceObject = 'connecting'
        yield logClient.tornadoInfoLog("trying to connect")
        try:
            self.serviceObject = yield websocket_connect(WS_HOST)
        except Exception as e:
            self.serviceObject = None
            yield self.closeAllClient()
            yield logClient.tornadoInfoLog("connection error")
        else:
            yield logClient.tornadoInfoLog("connected")
            self.ioloop.add_timeout(self.ioloop.time(), self.wsClientLoopRun)

    @gen.coroutine
    def wsClientLoopRun(self):
        '''接收websocket服务器的信息'''
        try:
            msg = yield self.serviceObject.read_message()
        except:
            msg = None
            # return
        if msg is None:
            yield logClient.tornadoInfoLog("connection closed")
            yield self.closeAllClient()
        else:
            msg = json.loads(msg)
            yield logClient.tornadoDebugLog('2---服务端信息：({})'.format(msg))
            yield self.forwardInformation(json_dumps(msg))
            self.ioloop.add_timeout(self.ioloop.time(), self.wsClientLoopRun)

    @gen.coroutine
    def wsClientKeep_alive(self):
        if self.serviceObject == None:
            self.wsClientConnect()
        else:
            self.heartDelayCount += 1
            if self.heartDelayCount >= 10:
                self.heartDelayCount = 0
                msg = {'type':'heartbeat'}
                self.sendToService(json_dumps(msg))

    #todo:发送信息给websocket服务端
    @gen.coroutine
    def sendToService(self, msg):
        try:
            self.serviceObject.write_message(msg)
        except Exception as e:
            yield self.closeAllClient()
            yield logClient.tornadoErrorLog(str(e))

    #todo:关闭所有连接客户端
    @gen.coroutine
    def closeAllClient(self):
        self.serviceObject = None
        for clientObject in self.clientObjectSet:
            clientObject.close()
            del clientObject
        else:
            self.clientObjectSet = set()

    #todo:信息转发
    @gen.coroutine
    def forwardInformation(self,msg):
        '''
        功能： 将收到的信息,找到connection_token 相同的连接客户端,同步给他
        1>  channel_feedback类型(物联服务器主动推送消息) 需要判断是否有查询任务在等待,有查询任务则不推送
        2>  control类型(控制返回的假状态),需要处理聚集同步的事务
        3>  query类型(本服务查询,物联服务器返回的真实状态),直接同步
        '''
        try:
            msg = json.loads(msg)
        except:
            return
        msg_connection_token = msg.get('connection_token')
        msg_used = False
        for clientObject in self.clientObjectSet:  # 遍历所有前端websocket连接用户
            # connection_token 过滤
            if clientObject.connection_token != msg_connection_token or msg_connection_token == None:
                continue
            msg_used = True
            #分类型处理
            type_ = msg['type']
            # channel_feedback类消息  -- 推送给connection_token相同的前端,如果有查询任务在等待,则不推送
            if type_ == 'channel_feedback':

                channel_guid = msg.get('guid')  # 获取同步信息guid信息
                if channel_guid == None:
                    continue
                # 如果通道guid不在查询字典中,说明可以推送给前端
                if channel_guid not in clientObject.query_task_dict.keys():
                    # yield logClient.tornadoDebugLog('没有等待查询任务...推送信息:{}'.format(channel_guid))
                    yield clientObject.sendToWebsocketClient(msg)
                else:
                    yield logClient.tornadoDebugLog('查询任务等待中...不推送:{}'.format(channel_guid))
            elif type_ == 'login':
                #获取登录结果
                ret = msg.get('ret',1)
                if ret == 0:
                    #已登录状态,退出上一次登录
                    if clientObject.logined == 1:
                        for last_connection_token in clientObject.last_connection_token:
                            logont_msg = {
                                'type': 'logout',
                                'connection_token': last_connection_token
                            }
                            yield self.sendToService(json_dumps(logont_msg))
                            yield logClient.tornadoWarningLog('删除上次登录token:{}'.format(last_connection_token))
                        else:
                            clientObject.last_connection_token = []
                    #未登录状态 标注：已经登录
                    else:
                        clientObject.logined = 1
                yield clientObject.sendToWebsocketClient(msg)
                # 其他类型,直接推送
            else:
                if type_ == 'device_switch':
                    print(msg)
                yield clientObject.sendToWebsocketClient(msg)
            #判断是否是聚集通道,如果是聚集通道有同步的需要做同步
            #只有控制聚集通道时,才同步

            if type_ == 'control':
                channel_guid = msg.get('guid')  # 获取同步信息guid信息
                if channel_guid == None:
                    continue
                # 获取聚集通达状态
                channel_status = msg.get('status')  # 获取聚集通道的状态
                #1: 尝试判断是否为聚集通道
                sync_info = self.sync_channel_dict.get(channel_guid)
                if sync_info != None:  # 聚集通道
                    # 尝试获取状态相同的同步通道类别
                    sync_channel_list = sync_info.get(channel_status)

                    if sync_channel_list == None:
                        continue

                    # 遍历所有通道同步的通道,发送假反馈给前端
                    for sync_channel_info in sync_channel_list:
                        guid = sync_channel_info.get('sync_channel_guid')
                        meeting_room_guid = sync_channel_info.get('meeting_room_guid')
                        status = sync_channel_info.get('sync_channel_status')
                        try:
                            status = int(status)
                        except:
                            pass
                        info = {
                            'ret': 0,
                            'type': 'control',
                            'guid': guid,
                            'meeting_room_guid':meeting_room_guid,
                            'status': status,
                            'connection_token': msg_connection_token
                        }
                        yield clientObject.sendToWebsocketClient(info)
                        # 添加查询任务
                        delay_time = self.sync_delay_dict.get(guid, 3)
                        new_later_task = clientObject.ioloop.add_timeout(self.ioloop.time() + delay_time,clientObject.query_status, guid,meeting_room_guid)
                        last_later_task = clientObject.query_task_dict.get(guid)
                        if last_later_task != None:
                            clientObject.ioloop.remove_timeout(last_later_task)
                        clientObject.query_task_dict[guid] = new_later_task
                #2: 尝试获取互斥
                for mutex_group in self.mutex_group_list:
                    channel_guid_list = mutex_group.get('channel_guid_list',[])
                    if channel_guid in channel_guid_list:
                        mutex_channel_info_dict = mutex_group.get('mutex_channel_info',[])
                        master_info = mutex_channel_info_dict.get(channel_guid)
                        if master_info == None:
                            continue
                        master_now_status = None
                        for key,value in master_info.items():
                            if value == channel_status:
                                master_now_status = key
                                break
                        if master_now_status != None:
                            if master_now_status == 'negative_value':
                                #其他通道取负值
                                other_channel_status = 'positive_value'
                            elif master_now_status == 'positive_value':
                                #其他通道取正值
                                other_channel_status = 'negative_value'
                            else:
                                continue
                            for guid,mutex_channel_info in mutex_channel_info_dict.items():
                                if guid != channel_guid:
                                    status = mutex_channel_info.get(other_channel_status)
                                    meeting_room_guid = mutex_channel_info.get('meeting_room_guid')
                                    try:
                                        status = int(status)
                                    except:
                                        pass
                                    info = {
                                        'ret': 0,
                                        'type': 'control',
                                        'guid': guid,
                                        'meeting_room_guid':meeting_room_guid,
                                        'status': status,
                                        'connection_token': msg_connection_token
                                    }
                                    yield clientObject.sendToWebsocketClient(info)
                                    # 添加查询任务
                                    delay_time = self.sync_delay_dict.get(guid, 3)
                                    new_later_task = clientObject.ioloop.add_timeout(self.ioloop.time() + delay_time,clientObject.query_status, guid,meeting_room_guid)
                                    last_later_task = clientObject.query_task_dict.get(guid)
                                    if last_later_task != None:
                                        clientObject.ioloop.remove_timeout(last_later_task)
                                    clientObject.query_task_dict[guid] = new_later_task
                        else:
                            continue
        else:
            if msg_used == False:
                logont_msg = {
                    'type': 'logout',
                    'connection_token': msg_connection_token
                }
                yield self.sendToService(json_dumps(logont_msg))
                yield logClient.tornadoWarningLog('删除上次登录token:{}'.format(msg_connection_token))