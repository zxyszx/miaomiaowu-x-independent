"""Shared Agent liveness window and exact session-bound receipt checks."""

ONLINE_WINDOW = 45
REQUEST_TIMEOUT = 10
POST_HEARTBEAT_REQUESTS = 4
DEADLINE_MARGIN = 1


def scheduling_interval(value):
    if (type(value) not in (int, float) or value <= 0
            or value + REQUEST_TIMEOUT * POST_HEARTBEAT_REQUESTS >= ONLINE_WINDOW):
        raise ValueError('Agent调度间隔必须连同网络上限严格小于离线窗口')
    return value


def session_receipt(value, server_id, session_id):
    expected = dict(accepted=True, server_id=server_id, session_id=session_id)
    if (type(value) is not dict or set(value) != set(expected)
            or value.get('accepted') is not True
            or type(value.get('server_id')) is not int or value != expected):
        raise ValueError('主控未精确确认 Agent 会话身份')
    return value


def heartbeat_receipt(value, server_id, session_id):
    expected = dict(accepted=True, server_id=server_id, session_id=session_id)
    if (type(value) is not dict or set(value) != set(expected)
            or value.get('accepted') is not True
            or type(value.get('server_id')) is not int or value != expected):
        raise ValueError('主控未精确确认 Agent 心跳身份')
    return value
