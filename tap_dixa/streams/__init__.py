from .activitylogs import ActivityLogs
from .conversations import Conversations
from .messages import Messages

STREAMS = {
    "activity_logs": ActivityLogs,
    "conversations": Conversations,
    "messages": Messages,
}