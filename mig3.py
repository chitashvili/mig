import os
import re
from pathlib import Path
from typing import Iterator, Dict, Pattern, Callable, Match

import MySQLdb

#  Constants
#  ---------

EXPORT_DIR = "./exported"

LINK_REGEX = re.compile(
    r"\[\[\s*(?P<url>.*\S)\s*\|\s*(?P<text>.*\S)\s*\]\]", re.MULTILINE)
LINK_SUBSTITUTION = "[\\2](\\1)"  # See: https://regex101.com/r/7p7YVw/2

INTERNAL_LINK_REGEX = re.compile(r"\[\[\s*(?P<slug>.*\S)\s*\]\]", re.MULTILINE)

# See: https://regex101.com/r/pRiKZW/1
HEADER_REGEX = re.compile(r"(=+) ?(.*) ?=+", re.MULTILINE)


#  Utils
#  -----

def ellipsis(s: str, n: int) -> str:
    s = s.replace("\n", "\t")
    if len(s) > n:
        return s[: n - 2] + ".."
    else:
        return s


def custom_re_sub(regex: Pattern, replacer: Callable[[Match, ], str], text) -> str:
    m = regex.search(text)
    while m is not None:
        text = text[:m.start()] + replacer(m) + text[m.end() + 1:]
        m = regex.search(text)

    return text


def regressive_relative_to(from_: Path, to: Path) -> Path:
    print(f"trying to join {from_} to {to}")
    try:
        result = to.relative_to(from_)
    except ValueError:
        result = Path('..', str(regressive_relative_to(from_.parent, to)))

    print("It gives:", result)
    return result


def render_link(from_: Path, to: Path, text: str) -> str:
    return f"[{text}]({regressive_relative_to(from_, to)})"


def format_slug(url: str) -> str:
    if url[-1] != '/':
        url += '/'

    return url.lower()


#  Document class
#  --------------


class Document:
    id: int
    version: int
    slug: str
    title: str
    content: str
    documentPHID: str
    path: Path

    def __init__(self, id: str, version: str, slug: str, title: str, content: str,
                 documentPHID: bytes):
        self.id = int(id)
        self.version = int(version)
        self.slug = slug
        self.title = title
        self.content = content
        self.documentPHID = documentPHID.decode()

        if slug == '/':
            self.path = Path(EXPORT_DIR)
        else:
            self.path = Path(EXPORT_DIR, slug)

        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def correct_path(self):
        if self.path.exists() and self.path.is_dir():
            self.path = self.path / "README.md"
        else:
            self.path = self.path.with_suffix(".md")

    def __str__(self):
        return (
            f"Document {self.documentPHID} \"{ellipsis(self.title, 20)}\" ({self.version})"
            f" at {self.slug}: {ellipsis(self.content, 40)} "
        )

    def __repr__(self):
        return f"Document {self.documentPHID} at {self.slug}"

    def clean_content(self, docs_by_slug):
        self.content = "# " + self.title + "\n\n" + self.content

        self.clean_links(docs_by_slug)
        self.clean_headers()

    def clean_links(self, docs_by_slug):
        def replace_a_raw_link(m: Match) -> str:
            url = m['url']
            text = m['text']
            slug = format_slug(url)
            if slug in docs_by_slug:
                d = docs_by_slug[slug]
                return render_link(self.path.parent, d.path, text)
            else:
                return f"[{text}]({url})"

        self.content = re.sub(LINK_REGEX, replace_a_raw_link, self.content)

        def replace_a_link_by_slug(m: Match) -> str:
            url = m['slug']
            slug = format_slug(url)
            if slug in docs_by_slug:
                d = docs_by_slug[slug]
                return render_link(self.path.parent, d.path, d.title)
            else:
                return f"[{slug}]({slug})"

        self.content = custom_re_sub(
            INTERNAL_LINK_REGEX, replace_a_link_by_slug, self.content)

    def clean_headers(self):
        def replace_a_header(m: Match) -> str:
            return "#" * len(m[1]) + " " + m[2] + "\n"

        self.content = custom_re_sub(
            HEADER_REGEX, replace_a_header, self.content)

    def get_filename(self, base_dir) -> str:
        if self.slug == '/':
            return os.path.join(base_dir, "README.md")

        dirs = self.slug.split('/')
        dirs = [s for s in dirs if len(s) > 0]

        if len(dirs) == 0:
            raise Exception("Bad slug here: ", self)

        filename = str(dirs[-1])
        paths = dirs[:-1]

        path = os.path.join(base_dir, *paths)
        os.makedirs(path, exist_ok=True)

        raw_complete = os.path.join(path, filename)

        if os.path.exists(raw_complete):
            return os.path.join(raw_complete, "README.md")
        else:
            return raw_complete + '.md'

    def save(self):
        with self.path.open("w") as f:
            f.write(self.content)


#   Access and filter functions
#   ---------------------------


def select_max_version_of_documents(docs: Iterator[Document]) -> Iterator[Document]:
    max_versions: Dict[str, Document] = dict()

    for doc in docs:
        if doc.documentPHID in max_versions:
            contestant = max_versions[doc.documentPHID]
            if contestant.version < doc.version:
                max_versions[doc.documentPHID] = doc
        else:
            max_versions[doc.documentPHID] = doc

    return (doc for doc in max_versions.values())


def fetch_documents(cursor):
    query = "SELECT id, version, slug, title, content, documentPHID FROM phriction_content"
    cursor.execute(query)
    return (Document(*row) for row in cursor.fetchall())


def get_documents_by_slug(docs: Iterator[Document]) -> Dict[str, Document]:
    result = dict()
    for d in docs:
        result[d.slug] = d

    return result


#   Main
#   ----

def main():
    db = MySQLdb.connect(user="hip", password="hop",
                         db="phabricator_phriction")
    all_docs = fetch_documents(db.cursor())
    filtered_docs = list(select_max_version_of_documents(all_docs))
    docs_by_slug = get_documents_by_slug(filtered_docs)
    for d in filtered_docs:
        d.correct_path()
    for d in filtered_docs:
        d.clean_content(docs_by_slug)
    for d in filtered_docs:
        d.save()


if __name__ == '__main__':
    main()
