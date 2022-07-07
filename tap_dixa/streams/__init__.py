from tap_dixa.streams.activitylogs import ActivityLogs
from tap_dixa.streams.conversations import Conversations
from tap_dixa.streams.messages import Messages

STREAMS = {
    "activity_logs": ActivityLogs,
    "conversations": Conversations,
    "messages": Messages,
}
