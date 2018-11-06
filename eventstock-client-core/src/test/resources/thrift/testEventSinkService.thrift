typedef i64 EventID

struct EventRange {
    1: optional EventID after
    2: required i32 limit
}

exception NoLastEvent {}

struct SinkEvent {
    1: required EventID eventId
    2: required string  created_at
}

service EventSink {

    list<SinkEvent> GetEvents (1: EventRange range)
        throws ()

    EventID GetLastEventID ()
        throws (1: NoLastEvent ex1)

}