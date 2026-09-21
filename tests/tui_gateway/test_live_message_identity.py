from tui_gateway.live_message_identity import LiveMessageIdentity


def test_live_message_identity_stays_stable_until_segment_boundary():
    identity = LiveMessageIdentity("turn-1")

    assert identity.message_id == "turn-1:assistant:0"
    assert identity.payload({"text": "partial"}) == {
        "text": "partial",
        "turn_id": "turn-1",
        "message_id": "turn-1:assistant:0",
    }

    next_identity = identity.next_segment()
    assert identity.message_id == "turn-1:assistant:0"
    assert next_identity.message_id == "turn-1:assistant:1"
