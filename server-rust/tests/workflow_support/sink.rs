//! Strict workflow test sink: unexpected issue events fail rather than silently disappear.
use multica_server::api::issue_write::{
    IssueAssignmentEvent, IssueCommentChangedEvent, IssueCommentCreatedEvent,
    IssueCommentResolutionEvent, IssueCreatedEvent, IssueDeletedEvent, IssueEventSink,
    IssueMetadataEvent, IssueReactionEvent, IssueSubscriberEvent, IssueTaskCancelledEvent,
    IssueUpdatedEvent, IssueWakeupActivityEvent, IssueWakeupDispatchRequest,
};

pub struct StrictSink;

impl IssueEventSink for StrictSink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {
        panic!("unexpected issue deleted event")
    }
    fn publish_issue_created(&self, _: IssueCreatedEvent) {
        panic!("unexpected issue created event")
    }
    fn publish_comment_created(&self, _: IssueCommentCreatedEvent) {
        panic!("unexpected comment created event")
    }
    fn publish_comment_changed(&self, _: IssueCommentChangedEvent) {
        panic!("unexpected comment changed event")
    }
    fn publish_metadata_changed(&self, _: IssueMetadataEvent) {
        panic!("unexpected metadata event")
    }
    fn publish_reaction_changed(&self, _: IssueReactionEvent) {
        panic!("unexpected reaction event")
    }
    fn publish_assignment_changed(&self, _: IssueAssignmentEvent) {
        panic!("unexpected assignment event")
    }
    fn publish_subscriber_changed(&self, _: IssueSubscriberEvent) {
        panic!("unexpected subscriber event")
    }
    fn publish_issue_updated(&self, _: IssueUpdatedEvent) {
        panic!("unexpected issue update event")
    }
    fn publish_comment_resolution(&self, _: IssueCommentResolutionEvent) {
        panic!("unexpected comment resolution event")
    }
    fn publish_wakeup_activity(&self, _: IssueWakeupActivityEvent) {
        panic!("unexpected wakeup event")
    }
    fn publish_task_cancelled(&self, _: IssueTaskCancelledEvent) {
        panic!("unexpected task cancellation event")
    }
    fn request_wakeup_dispatch(&self, _: IssueWakeupDispatchRequest) {
        panic!("unexpected wakeup dispatch")
    }
}
