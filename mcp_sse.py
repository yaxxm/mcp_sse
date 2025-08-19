#解决了时间新增格式、策略号、gameid多选
#-----------------------最终版---------------------
#不告诉你（姚明俊）
#功能包括
#   查询策略的命中量，
#   策略一段时间的每日命中量，
#   命中具体账单，
#   game_id转game_name
"""
策略处罚数据查询 MCP 服务器
"""
from mcp.server import Server
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount, Route
import uvicorn
import requests
import re
import json
import datetime
#-----------------------辅助模块------------------------------
mcp = FastMCP("策略处罚数据查询工具")

#字典
status_dict = {
    "全量": [9999999],  #对应value为空
    "处罚成功": [0, 5023, 5024, 5025, 5011, 3013],
    "静默": [3006, 3008],
    "灰度": [3007],
    "处罚失败": [999999999998]  #进入if判断 执行排除
}
# 辅助函数：处理punish_time参数
def parse_punish_time(punish_time, now_time):
    """解析punish_time参数，支持数字（天数）、YYYY-MM-DD格式或YYYY.M.D-YYYY.M.D格式
    
    Args:
        punish_time: 可以是None、数字（天数）、YYYY-MM-DD格式或YYYY.M.D-YYYY.M.D格式的字符串
        now_time: 当前时间
    
    Returns:
        datetime对象，表示开始时间
    """
    if punish_time is None or punish_time == "":
        return now_time - datetime.timedelta(days=30)
    elif isinstance(punish_time, int):
        return now_time - datetime.timedelta(days=punish_time)
    elif isinstance(punish_time, str):
        # 检查是否为日期范围格式 YYYY.M.D-YYYY.M.D
        if '-' in punish_time and '.' in punish_time:
            try:
                start_str, end_str = punish_time.split('-', 1)
                # 解析开始日期
                start_parts = start_str.strip().split('.')
                if len(start_parts) == 3:
                    start_date = datetime.datetime(int(start_parts[0]), int(start_parts[1]), int(start_parts[2]))
                    return start_date
            except (ValueError, IndexError):
                pass
        
        # 尝试解析YYYY-MM-DD格式
        try:
            parsed_date = datetime.datetime.strptime(punish_time, "%Y-%m-%d")
            return parsed_date
        except ValueError:
            # 尝试解析YYYY.M.D格式
            try:
                parts = punish_time.split('.')
                if len(parts) == 3:
                    parsed_date = datetime.datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                    return parsed_date
            except (ValueError, IndexError):
                pass
            
            # 如果不是日期格式，尝试转换为数字
            try:
                days = int(punish_time)
                return now_time - datetime.timedelta(days=days)
            except ValueError:
                # 如果都不是，使用默认值
                return now_time - datetime.timedelta(days=30)
    else:
        return now_time - datetime.timedelta(days=30)

# 辅助函数：将punish_time转换为天数（用于get_daily_scheme_hits函数）
def parse_punish_time_to_days(punish_time, now_time):
    """解析punish_time参数并转换为天数
    
    Args:
        punish_time: 可以是None、数字（天数）、YYYY-MM-DD格式或YYYY.M.D-YYYY.M.D格式的字符串
        now_time: 当前时间
    
    Returns:
        整数，表示天数
    """
    if punish_time is None or punish_time == "":
        return 7
    elif isinstance(punish_time, int):
        return punish_time
    elif isinstance(punish_time, str):
        # 检查是否为日期范围格式 YYYY.M.D-YYYY.M.D
        if '-' in punish_time and '.' in punish_time:
            try:
                start_str, end_str = punish_time.split('-', 1)
                # 解析开始日期和结束日期
                start_parts = start_str.strip().split('.')
                end_parts = end_str.strip().split('.')
                if len(start_parts) == 3 and len(end_parts) == 3:
                    start_date = datetime.datetime(int(start_parts[0]), int(start_parts[1]), int(start_parts[2]))
                    end_date = datetime.datetime(int(end_parts[0]), int(end_parts[1]), int(end_parts[2]))
                    days_diff = (end_date.date() - start_date.date()).days + 1  # 包含结束日期
                    return max(1, days_diff)
            except (ValueError, IndexError):
                pass
        
        # 尝试解析YYYY-MM-DD格式
        try:
            parsed_date = datetime.datetime.strptime(punish_time, "%Y-%m-%d")
            days_diff = (now_time.date() - parsed_date.date()).days
            return max(1, days_diff)  # 至少返回1天
        except ValueError:
            # 尝试解析YYYY.M.D格式
            try:
                parts = punish_time.split('.')
                if len(parts) == 3:
                    parsed_date = datetime.datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                    days_diff = (now_time.date() - parsed_date.date()).days
                    return max(1, days_diff)
            except (ValueError, IndexError):
                pass
            
            # 如果不是日期格式，尝试转换为数字
            try:
                days = int(punish_time)
                return days
            except ValueError:
                # 如果都不是，使用默认值
                return 7
    else:
        return 7

# 辅助函数：解析punish_time参数获取结束时间
def parse_punish_end_time(punish_time, now_time):
    """解析punish_time参数获取结束时间，支持日期范围格式
    
    Args:
        punish_time: 可以是None、数字（天数）、YYYY-MM-DD格式或YYYY.M.D-YYYY.M.D格式的字符串
        now_time: 当前时间
    
    Returns:
        datetime对象，表示结束时间
    """
    if punish_time is None or punish_time == "":
        return now_time
    elif isinstance(punish_time, int):
        return now_time
    elif isinstance(punish_time, str):
        # 检查是否为日期范围格式 YYYY.M.D-YYYY.M.D
        if '-' in punish_time and '.' in punish_time:
            try:
                start_str, end_str = punish_time.split('-', 1)
                # 解析结束日期
                end_parts = end_str.strip().split('.')
                if len(end_parts) == 3:
                    end_date = datetime.datetime(int(end_parts[0]), int(end_parts[1]), int(end_parts[2]))
                    # 设置为当天的23:59:59
                    end_date = end_date.replace(hour=23, minute=59, second=59)
                    return end_date
            except (ValueError, IndexError):
                pass
        
        # 对于其他格式，返回当前时间
        return now_time
    else:
        return now_time

# 辅助函数：根据punish_time参数生成日期列表（用于get_daily_scheme_hits函数）
def generate_date_list(punish_time, now_time):
    """根据punish_time参数生成日期列表
    
    Args:
        punish_time: 可以是None、数字（天数）、YYYY-MM-DD格式或YYYY.M.D-YYYY.M.D格式的字符串
        now_time: 当前时间
    
    Returns:
        日期列表，每个元素为datetime对象
    """
    if punish_time is None or punish_time == "":
        # 默认7天
        return [now_time - datetime.timedelta(days=i) for i in range(6, -1, -1)]
    elif isinstance(punish_time, int):
        # 按天数生成
        return [now_time - datetime.timedelta(days=i) for i in range(punish_time - 1, -1, -1)]
    elif isinstance(punish_time, str):
        # 检查是否为日期范围格式 YYYY.M.D-YYYY.M.D
        if '-' in punish_time and '.' in punish_time:
            try:
                start_str, end_str = punish_time.split('-', 1)
                # 解析开始日期和结束日期
                start_parts = start_str.strip().split('.')
                end_parts = end_str.strip().split('.')
                if len(start_parts) == 3 and len(end_parts) == 3:
                    start_date = datetime.datetime(int(start_parts[0]), int(start_parts[1]), int(start_parts[2]))
                    end_date = datetime.datetime(int(end_parts[0]), int(end_parts[1]), int(end_parts[2]))
                    # 生成日期范围内的所有日期
                    date_list = []
                    current_date = start_date
                    while current_date <= end_date:
                        date_list.append(current_date)
                        current_date += datetime.timedelta(days=1)
                    return date_list
            except (ValueError, IndexError):
                pass
        
        # 尝试解析其他格式，转换为天数
        try:
            # 尝试解析YYYY-MM-DD格式
            parsed_date = datetime.datetime.strptime(punish_time, "%Y-%m-%d")
            days_diff = (now_time.date() - parsed_date.date()).days + 1
            return [now_time - datetime.timedelta(days=i) for i in range(days_diff - 1, -1, -1)]
        except ValueError:
            try:
                # 尝试解析YYYY.M.D格式
                parts = punish_time.split('.')
                if len(parts) == 3:
                    parsed_date = datetime.datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                    days_diff = (now_time.date() - parsed_date.date()).days + 1
                    return [now_time - datetime.timedelta(days=i) for i in range(days_diff - 1, -1, -1)]
            except (ValueError, IndexError):
                pass
            
            # 尝试转换为数字
            try:
                days = int(punish_time)
                return [now_time - datetime.timedelta(days=i) for i in range(days - 1, -1, -1)]
            except ValueError:
                # 默认7天
                return [now_time - datetime.timedelta(days=i) for i in range(6, -1, -1)]
    else:
        # 默认7天
        return [now_time - datetime.timedelta(days=i) for i in range(6, -1, -1)]

# 工具函数：处理单行数据
def process_single_row(row):
    """处理单行数据"""
    try:
        # 提取所有字段（按column_list顺序）
        log_time = row[0]
        gameid = row[2]
        status = row[4]  # 对应result字段
        punish_src = row[5]
        schemeid = row[6]
        level = row[8]
        is_offline = row[9]
        kv_data_str = row[12]
        account = row[13]  # 对应account_id
        account_type = row[14]
        plat_id = row[15]
        world_id = row[16]
        
        # 解析kv_data
        kv_data = json.loads(kv_data_str)
        evidence = kv_data.get("evidence", "")
        if len(evidence.split('::', 1)) < 2:
            checked_case = evidence
        else:
            checked_case = evidence.split('::', 1)[1]
        
        return {
            "log_time": f"{log_time}",
            "game_id": f"{gameid}",
            "result": f"{status}",
            #"punish_src": f"{punish_src}",
            "scheme": f"{schemeid}",
            "level": f"{level}",
            #"is_offline": f"{is_offline}",
            "account_id": f"{account}",
            #"account_type": f"{account_type}",
            #"plat_id": f"{plat_id}",
            #"world_id": f"{world_id}",
            "punish_text": f"{checked_case}"
        }
    except Exception as e:
        return {"error": f"处理数据行时出错: {str(e)}"}

#----------------------辅助模块------------------------------
#---------------------获取游戏策略命中数量-----------------
@mcp.tool()
def check_publish_data(game_id=None, punish_time:str=None, scheme_id=None, status:str=None, count_type:str="条"):
    """函数工具描述：通过给定的信息，从处罚账单里捞取策略的处罚量
    处罚时间默认30天
    输入参数：
    game_id：游戏id，支持单个值、逗号分隔的多个值、或None(全选)
    punish_time：处罚时间，支持数字（天数）、YYYY-MM-DD格式或YYYY.M.D-YYYY.M.D格式
    scheme_id：策略id，支持单个值、逗号分隔的多个值、或None(全选)
    status：处罚状态，可选值：全量、处罚成功、静默、灰度、处罚失败
    count_type：计算类型，'条'按返回条数计算，'账号'按distinct open_id计算
    """
    url=''#/自己补充自己的
    now_time = datetime.datetime.now()
    
    # 处理处罚时间参数
    punish_time_obj = parse_punish_time(punish_time, now_time)
    end_time_obj = parse_punish_end_time(punish_time, now_time)
    start_format_time = punish_time_obj.strftime("%Y-%m-%d %H:%M:%S")
    end_format_time = end_time_obj.strftime("%Y-%m-%d %H:%M:%S")

    # 处理game_id参数（支持多选）
    game_ids = []
    if game_id is not None:
        if isinstance(game_id, str):
            game_ids = [g.strip() for g in game_id.split(',') if g.strip()]
        elif isinstance(game_id, list):
            game_ids = [str(g) for g in game_id]
        else:
            game_ids = [str(game_id)]
    
    # 处理scheme_id参数（支持多选）
    scheme_ids = []
    if scheme_id is not None:
        if isinstance(scheme_id, str):
            scheme_ids = [s.strip() for s in scheme_id.split(',') if s.strip()]
        elif isinstance(scheme_id, list):
            scheme_ids = [str(s) for s in scheme_id]
        else:
            scheme_ids = [str(scheme_id)]
    
    total_count = 0
    unique_accounts = set()
    detailed_results = []  # 存储每个策略的详细结果
    
    # 如果game_ids为空，表示全选，需要查询所有游戏
    if not game_ids:
        game_ids = [None]  # None表示不添加game_id过滤条件
    
    # 如果scheme_ids为空，表示全选，需要查询所有策略
    if not scheme_ids:
        scheme_ids = [None]  # None表示不添加scheme_id过滤条件
    
    # 遍历所有game_id和scheme_id的组合
    for gid in game_ids:
        for sid in scheme_ids:
            # 构建查询条件
            game_filter_cond = '{"field":"game_id","cond":"0","value":"%s","comment":"","col_alias":"game_id","is_custom_col":0,"type":"BIGINT","is_vaild":1}' % (gid) if gid else ""
            scheme_filter_cond = '{"field":"scheme","cond":"0","value":"%s","comment":"","col_alias":"scheme","is_custom_col":0,"type":"BIGINT","is_vaild":1}' % (sid) if sid else ""
            
            # 根据status参数动态设置result_cond
            if status and status in status_dict:
                if status == "全量":
                    result_cond = ''
                elif status == "处罚失败":
                    # 排除其他状态的值
                    exclude_values = [0, 5023, 5024, 5025, 5011, 3013, 3006, 3008, 3007]
                    exclude_str = ','.join(map(str, exclude_values))
                    result_cond = '{"field":"result","cond":"1","value":"' + exclude_str + '","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
                else:
                    # 其他状态：处罚成功、静默、灰度
                    status_values = status_dict[status]
                    value_str = ','.join(map(str, status_values))
                    result_cond = '{"field":"result","cond":"0","value":"' + value_str + '","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
            else:
                result_cond = '{"field":"result","cond":"0","value":"0","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'

            conditon_list = [game_filter_cond, scheme_filter_cond]
            if result_cond:  # 只有当result_cond不为空时才添加
                conditon_list.append(result_cond)

            # 组装过滤条件
            filter_conds = '['
            for cond in conditon_list:
                if len(cond) > 0:
                    filter_conds += cond + ','
            filter_conds = filter_conds[:-1] if len(filter_conds) > 3 else filter_conds
            filter_conds += ']'
    
            # 根据count_type设置col_conds
            if count_type == "账号":
                col_conds = '[{"col_name":"account_id","calc":"distinct_count","type":"col","order_cond":"default","condition":"","comment":"","is_custom_col":0,"col_alias":"account_id","col_type":"STRING","col_desc":"账号"}]'
            else:
                col_conds = '[]'
            
            # 构建请求参数
            param_dict = {
                'text_conds': '[{"query_str": ""}]',
                'view_type': '不告诉你',
                'login_user': '不告诉你',
                'view_id': '不告诉你',
                'node_id': 0,
                'filter_conds': filter_conds,
                'col_conds': col_conds,
                'custom_col_datas': '[]',
                'row_conds': '[]',
                'token': '7b14d4958ffb2c2845f98ce9a19e23a1',
                'factor_conds': '[]',
                'table_name': '不告诉你_*',
                'order_conds': '["log_time","DESC",0,"NoComment"]',
                'act': 'update_data_chart',
                'page_conds': '[1, 10000]',  # 增加页面大小以获取更多数据
                'datetime_conds': '["@timestamp","YYYY-MM-DD HH:MM:SS","%s","%s"]' % (start_format_time, end_format_time)
            }

            # 发送请求并处理结果
            try:
                res = requests.post(url, data=param_dict)
                d = json.loads(res.content)
                if d["msg"] == "ok":
                    if count_type == "条":
                        # 按条数计算
                        data_num = d["data"]["data_count"]
                        total_count += data_num
                        # 记录详细结果
                        game_label = f"游戏{gid}" if gid else "全部游戏"
                        scheme_label = f"策略{sid}" if sid else "全部策略"
                        detailed_results.append(f"{game_label}的{scheme_label}命中量: {data_num}条")
                    elif count_type == "账号":
                        # 直接从data_list中读取distinct_count结果
                        distinct_count = 0
                        if "data_list" in d["data"] and d["data"]["data_list"] and len(d["data"]["data_list"]) > 0:
                            if len(d["data"]["data_list"][0]) > 0:
                                distinct_count = int(d["data"]["data_list"][0][0]) if d["data"]["data_list"][0][0] is not None else 0
                        
                        total_count += distinct_count
                        # 记录详细结果
                        game_label = f"游戏{gid}" if gid else "全部游戏"
                        scheme_label = f"策略{sid}" if sid else "全部策略"
                        detailed_results.append(f"{game_label}的{scheme_label}命中账号数: {distinct_count}个")
                else:
                    continue  # 如果查询失败，继续下一个组合
            except Exception as e:
                continue  # 如果出错，继续下一个组合
    
    # 返回结果
    game_desc = "全部游戏" if not game_ids or game_ids == [None] else ",".join([str(g) for g in game_ids if g])
    scheme_desc = "全部策略" if not scheme_ids or scheme_ids == [None] else ",".join([str(s) for s in scheme_ids if s])
    
    # 构建详细结果字符串
    result_lines = [f'{punish_time}到现在的命中情况:']
    
    # 添加每个策略的详细结果
    if detailed_results:
        result_lines.extend(detailed_results)
    
    # 添加总计结果
    if count_type == "条":
        result_lines.append(f'总计: 游戏{game_desc}的策略{scheme_desc}命中量为{total_count}条')
    elif count_type == "账号":
        result_lines.append(f'总计: 游戏{game_desc}的策略{scheme_desc}命中唯一账号数为{total_count}个')
    
    return '\n'.join(result_lines)
#--------------------获取游戏策略命中数量------------------

#-----------------------日命中数量-------------------------------------------
@mcp.tool()
def get_daily_scheme_hits(game_id: str = None, scheme_id: str = None, punish_time: str = None, status: str = None, count_type: str = "条"):
    """函数工具描述：获取指定game_id和scheme_id在指定时间范围内每天的策略命中量
    输入参数：
    game_id: 游戏ID，支持单个ID、多个ID（逗号分隔）或None（全选），默认为全选
    scheme_id: 策略ID，支持单个ID、多个ID（逗号分隔）或None（全选），默认为全选
    punish_time: 查询时间，支持数字（天数）、YYYY-MM-DD格式或YYYY.M.D-YYYY.M.D格式，默认为7天
    status：处罚状态，可选值：全量、处罚成功、静默、灰度、处罚失败
    count_type: 计算方式，"条"按返回条数计算，"账号"按distinct account_id计算，默认为"条"
    返回: 包含N+1个JSON的列表，前N个为每个策略号的结果，最后1个为总计结果
    """
    url=“”#自己补充自己的
    now_time = datetime.datetime.now()
    results = []  # 存储所有结果
    
    # 处理punish_time参数，生成日期列表
    date_list = generate_date_list(punish_time, now_time)
    days = len(date_list)
    
    # 解析game_id参数
    if game_id is None or game_id == "" or game_id == "全选":
        game_ids = [None]  # None表示全选
    else:
        game_ids = [gid.strip() for gid in str(game_id).split(',') if gid.strip()]
        if not game_ids:
            game_ids = [None]
    
    # 解析scheme_id参数
    if scheme_id is None or scheme_id == "" or scheme_id == "全选":
        scheme_ids = [None]  # None表示全选
    else:
        scheme_ids = [sid.strip() for sid in str(scheme_id).split(',') if sid.strip()]
        if not scheme_ids:
            scheme_ids = [None]
    
    # 初始化总计数据
    total_daily_hits = []
    
    # 遍历所有game_id和scheme_id的组合，为每个组合生成单独的结果
    for gid in game_ids:
        for sid in scheme_ids:
            # 为当前策略号创建单独的结果
            scheme_daily_hits = []
            scheme_name = f"游戏ID:{gid if gid else '全选'}_策略ID:{sid if sid else '全选'}"
            
            # 遍历日期列表，每天单独查询
            for idx, target_date in enumerate(date_list):
                # 计算当天的开始和结束时间
                start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                
                start_format_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                end_format_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # 初始化当天的统计
                daily_count = 0
                
                # 构建查询条件
                conditon_list = []
                
                if gid is not None:
                    game_filter_cond = '{"field":"game_id","cond":"0","value":"%s","comment":"","col_alias":"game_id","is_custom_col":0,"type":"BIGINT","is_vaild":1}' % (gid)
                    conditon_list.append(game_filter_cond)
                
                if sid is not None:
                    scheme_filter_cond = '{"field":"scheme","cond":"0","value":"%s","comment":"","col_alias":"scheme","is_custom_col":0,"type":"BIGINT","is_vaild":1}' % (sid)
                    conditon_list.append(scheme_filter_cond)
                
                # 根据status参数动态设置result_cond
                if status and status in status_dict:
                    if status == "全量":
                        result_cond = ''
                    elif status == "处罚失败":
                        # 排除其他状态的值
                        exclude_values = [0, 5023, 5024, 5025, 5011, 3013, 3006, 3008, 3007]
                        exclude_str = ','.join(map(str, exclude_values))
                        result_cond = '{"field":"result","cond":"1","value":"' + exclude_str + '","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
                    else:
                        # 其他状态：处罚成功、静默、灰度
                        status_values = status_dict[status]
                        value_str = ','.join(map(str, status_values))
                        result_cond = '{"field":"result","cond":"0","value":"' + value_str + '","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
                else:
                    result_cond = '{"field":"result","cond":"0","value":"0","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
                
                if result_cond:  # 只有当result_cond不为空时才添加
                    conditon_list.append(result_cond)
                
                # 组装过滤条件
                filter_conds = '['
                for cond in conditon_list:
                    if len(cond) > 0:
                        filter_conds += cond + ','
                filter_conds = filter_conds[:-1] if len(filter_conds) > 3 else filter_conds
                filter_conds += ']'
                
                # 根据count_type设置col_conds
                if count_type == "账号":
                    col_conds = '[{"col_name":"account_id","calc":"distinct_count","type":"col","order_cond":"default","condition":"","comment":"","is_custom_col":0,"col_alias":"account_id","col_type":"STRING","col_desc":"账号"}]'
                else:
                    col_conds = '[]'
                
                # 构建请求参数
                param_dict = {
                    'text_conds': '[{"query_str": ""}]',
                    'view_type': '不告诉你',
                    'login_user': '不告诉你',
                    'view_id': '不告诉你',
                    'node_id': 0,
                    'filter_conds': filter_conds,
                    'col_conds': col_conds,
                    'custom_col_datas': '[]',
                    'row_conds': '[]',
                    'token': '7b14d4958ffb2c2845f98ce9a19e23a1',
                    'factor_conds': '[]',
                    'table_name': '不告诉你_*',
                    'order_conds': '["log_time","DESC",0,"NoComment"]',
                    'act': 'update_data_chart',
                    'page_conds': '[1, 1000]',
                    'datetime_conds': '["@timestamp","YYYY-MM-DD HH:MM:SS","%s","%s"]' % (start_format_time, end_format_time)
                }
                
                # 发送请求并处理结果
                try:
                    res = requests.post(url, data=param_dict)
                    d = json.loads(res.content)
                    
                    if d["msg"] == "ok":
                        if count_type == "条":
                            # 按条数计算
                            data_num = d["data"]["data_count"]
                            daily_count += data_num
                        elif count_type == "账号":
                            # 直接从data_list中读取distinct_count结果
                            if "data_list" in d["data"] and d["data"]["data_list"] and len(d["data"]["data_list"]) > 0:
                                if len(d["data"]["data_list"][0]) > 0:
                                    distinct_count = int(d["data"]["data_list"][0][0]) if d["data"]["data_list"][0][0] is not None else 0
                                    daily_count = distinct_count
                except Exception as e:
                    continue  # 如果出错，继续下一个组合
                
                # 添加当天的结果到当前策略的数据中
                scheme_daily_hits.append({
                    "date": target_date.strftime("%Y-%m-%d"),
                    "hits": daily_count
                })
                
                # 累加到总计数据中
                if len(total_daily_hits) <= idx:
                    total_daily_hits.extend([{"date": "", "hits": 0}] * (idx + 1 - len(total_daily_hits)))
                
                if len(total_daily_hits) > idx:
                    total_daily_hits[idx]["date"] = target_date.strftime("%Y-%m-%d")
                    total_daily_hits[idx]["hits"] += daily_count
            
            # 为当前策略号添加结果
            results.append({
                "scheme_name": scheme_name,
                "daily_hits": scheme_daily_hits
            })
    

    
    # 添加总计结果
    results.append({
        "scheme_name": "总计",
        "daily_hits": total_daily_hits
    })
    
    return results
#------------------------日命中数量-----------------------------------------

#----------------------命中账单-------------------------------
#--------------------------------命中账单------------------------
@mcp.tool()
def check_account_publish_data(game_id:str=None, punish_time:str=None, scheme_id:str=None, status:str=None):
    """函数工具描述：通过给定的信息，从处罚账单里捞取策略的日志详情
    处罚时间默认30天
    输入参数：
    game_id：游戏id或者游戏昵称
    punish_time：处罚时间，支持数字（天数）或YYYY-MM-DD格式
    scheme_id：策略id
    status：处罚状态，可选值：全量、处罚成功、静默、灰度、处罚失败
    """
    url=“”#自己补充自己的
    now_time = datetime.datetime.now()
    end_format_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # 处理处罚时间参数
    punish_time_obj = parse_punish_time(punish_time, now_time)
    start_format_time = punish_time_obj.strftime("%Y-%m-%d %H:%M:%S")
    
    # 构建查询条件
    game_filter_cond = '{"field":"game_id","cond":"0","value":"%s","comment":"","col_alias":"game_id","is_custom_col":0,"type":"BIGINT","is_vaild":1}' % (game_id) if game_id else ""
    scheme_filter_cond = '{"field":"scheme","cond":"0","value":"%s","comment":"","col_alias":"scheme","is_custom_col":0,"type":"BIGINT","is_vaild":1}' % (scheme_id) if scheme_id else ""
    # punish_src_cond = '{"field":"punish_src","cond":"0","value":"21","comment":"","col_alias":"punish_src","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
    # level_cond = '{"field":"level","cond":"0","value":"0","comment":"","col_alias":"level","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
    
    # 根据status参数动态设置result_cond
    if status and status in status_dict:
        if status == "全量":
            result_cond = ''
        elif status == "处罚失败":
            # 排除其他状态的值
            exclude_values = [0, 5023, 5024, 5025, 5011, 3013, 3006, 3008, 3007]
            exclude_str = ','.join(map(str, exclude_values))
            result_cond = '{"field":"result","cond":"1","value":"' + exclude_str + '","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
        else:
            # 其他状态：处罚成功、静默、灰度
            status_values = status_dict[status]
            value_str = ','.join(map(str, status_values))
            result_cond = '{"field":"result","cond":"0","value":"' + value_str + '","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'
    else:
        result_cond = '{"field":"result","cond":"0","value":"0","comment":"","col_alias":"result","is_custom_col":0,"type":"BIGINT","is_vaild":1}'

    # conditon_list = [game_filter_cond, punish_src_cond, scheme_filter_cond, level_cond]
    conditon_list = [game_filter_cond, scheme_filter_cond]
    if result_cond:  # 只有当result_cond不为空时才添加
        conditon_list.append(result_cond)

    # 组装过滤条件
    filter_conds = '['
    for cond in conditon_list:
        if len(cond) > 0:
            filter_conds += cond + ','
    filter_conds = filter_conds[:-1] if len(filter_conds) > 3 else filter_conds
    filter_conds += ']'

    # 构建请求参数
    param_dict = {
        'text_conds': '[{"query_str": ""}]',
        'view_type': '不告诉你',
        'login_user': '不告诉你',
        'view_id': '不告诉你',
        'node_id': 0,
        'filter_conds': filter_conds,
        'col_conds': '[]',
        'custom_col_datas': '[]',
        'row_conds': '[]',
        'token': '7b14d4958ffb2c2845f98ce9a19e23a1',
        'factor_conds': '[]',
        'table_name': '不告诉你_*',
        'order_conds': '["log_time","DESC",0,"NoComment"]',
        'act': 'update_data_chart',
        'page_conds': '[1, 100]',
        'datetime_conds': '["@timestamp","YYYY-MM-DD HH:MM:SS","%s","%s"]' % (start_format_time, end_format_time)
    }

    # 发送请求并处理结果
    try:
        res = requests.post(url, data=param_dict)
        d = json.loads(res.content)
        if d["msg"] == "ok":
            data_num = d["data"]["data_count"]
            if data_num == 0:
                return json.dumps([], ensure_ascii=False, indent=2)
            # 处理所有数据行
            all_results = []
            count=0
            for row in d["data"]["data_list"]:
                processed_row = process_single_row(row)
                all_results.append(processed_row)
                count+=1
                if count==5:
                    break
            # return json.dumps(all_results, ensure_ascii=False, indent=2)
            return all_results
        else:
            return json.dumps({"error": "查询失败，返回状态异常"}, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"请求处理出错: {str(e)}"}, ensure_ascii=False, indent=2)
#--------------------------------命中账单-----------------------
#---------------------命中账单--------------------------------
#--------------------获取游戏id----------------------
#----------根据游戏名查找游戏id---------------------------------------------

@mcp.tool()
def search_game_id_by_game_name(game_name: str):
    """根据游戏名查询游戏id
    参数：
        game_name: 游戏名
    
    返回:
        游戏id: XX  游戏名: XX
    """
    import requests
    import json

    response = requests.get(url="http://light.woa.com/v1/igame/get", cookies={"token": "9e01ca321803388b2058402e7a6c2189"})
    if response.status_code != 200:
        return f"http://light.woa.com/v1/igame/get链接请求失败，status_code: {response.status_code}"
    else:
        game_id_name_map = json.loads(response.content)

        for game in game_id_name_map["data"]:
            game_id = game["game_id"]
            if game_name == game["game_name"]:
                return game_id
#----------根据游戏名查找游戏id---------------------------------------------

# SSE传输配置
def create_starlette_app(mcp_server: Server, *, debug: bool = False) -> Starlette:
    """创建支持SSE的Starlette应用"""
    sse = SseServerTransport("/messages/")

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
                request.scope,
                request.receive,
                request._send,  # noqa: SLF001
        ) as (read_stream, write_stream):
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options(),
            )

    return Starlette(
        debug=debug,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

# 启动服务器
if __name__ == "__main__":
    mcp_server = mcp._mcp_server
    starlette_app = create_starlette_app(mcp_server, debug=True)
    # 端口号设置为7600
    uvicorn.run(starlette_app, host="XX.XX.XXX.XX", port=7602)


#------------------------测试-------------------
#--------------------------账单单日命中-------------------------------
# print(check_publish_data(game_id='2577', punish_time='2025.8.12-2025.8.13',scheme_id='170606',count_type='账号',status='全量'))
# print(check_publish_data(game_id='2577', punish_time='2025.8.12-2025.8.13',scheme_id='170606',count_type='条',status='全量'))
# # eg：
#             # 2025.8.12-2025.8.13到现在的命中情况:
#             # 游戏2577的策略170606命中账号数: 63219个
#             # 总计: 游戏2577的策略170606命中唯一账号数为63219个
#             # 2025.8.12-2025.8.13到现在的命中情况:
#             # 游戏2577的策略170606命中量: 680956条
#             # 总计: 游戏2577的策略170606命中量为680956条
# #print(check_publish_data( game_id='2577',punish_time=2,count_type='条',status='全量'))
# print('---------------------------------------------------------------')
# #------------------------区间内每日命中测试
# # def get_daily_scheme_hits(game_id: str = None, scheme_id: str = None, punish_time: str = None, status: str = None, count_type: str = "条"):
# print(get_daily_scheme_hits(game_id='2577', punish_time='2025.8.12-2025.8.13',scheme_id='170606',count_type='条',status='全量'))
# print(get_daily_scheme_hits(game_id='2577', punish_time='2025.8.12-2025.8.13',scheme_id='170606',count_type='账号',status='全量'))
