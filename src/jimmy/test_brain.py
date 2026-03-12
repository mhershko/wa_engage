from jimmy.brain import (
    _extract_forced_context_titles,
    _has_lecture_upload_source,
    _is_lecture_upload_site_question,
)


def test_detects_lecture_upload_site_question_pattern() -> None:
    assert _is_lecture_upload_site_question("איך מעלים הרצאות לאתר")
    assert _is_lecture_upload_site_question("צריך העלאה של הרצאה באתר")
    assert not _is_lecture_upload_site_question("איך מעלים סרטון ליוטיוב")


def test_requires_upload_site_source_family() -> None:
    assert _has_lecture_upload_source(["העלאת הרצאות לאתר"])
    assert _has_lecture_upload_source(["מדריך העלאת הרצאה לאתר המערכת"])
    assert not _has_lecture_upload_source(["העלאת הרצאות ליוטיוב"])


def test_extract_forced_context_titles() -> None:
    forced_context = (
        "Selected page title: העלאת הרצאות לאתר\n\nתוכן א'\n\n---\n\n"
        "Selected page title: בניית סילבוס\n\nתוכן ב'"
    )
    assert _extract_forced_context_titles(forced_context) == [
        "העלאת הרצאות לאתר",
        "בניית סילבוס",
    ]
