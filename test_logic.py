from app.pipeline import digest, make_summary, make_tags

def test_digest_is_stable_and_content_based():
    assert digest("hello") == digest("hello")
    assert digest("hello") != digest("hello!")

def test_mock_summary_uses_title_and_content():
    assert make_summary("Title", "one two three").startswith("Title:")

def test_tags_are_unique_and_bounded():
    tags = make_tags("Python backend backend services API")
    assert len(tags) <= 5
    assert len(tags) == len(set(tags))
