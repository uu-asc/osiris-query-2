import re
import json
from pathlib import Path
from typing import Any, Callable

from jinja2 import Environment, FileSystemLoader
from markdown import Markdown
import pandas as pd

from query.utils import DotDict, TS
from query.config import get_paths_from_config


def render_template(value, context):
    env = Environment()
    template = env.from_string(value)
    rendered = template.render(context)
    return rendered


class Report:
    TEMPLATES: get_paths_from_config('templates')

    def __init__(
        self,
        template_path: str|Path = '',
        post: Callable|list[Callable]|None = None,
    ):
        template_paths = [
            template_path,
            *self.TEMPLATES,
        ]
        self.env = Environment(
            loader = FileSystemLoader(template_paths),
            trim_blocks = True,
            lstrip_blocks = True,
        )
        self.env.policies['json.dumps_kwargs'] = {'sort_keys': False}

        if post is None:
            self.post = []
        elif isinstance(post, Callable):
            self.post = [post]
        elif isinstance(post, list):
            self.post = post
        else:
            raise TypeError("Post is not of type `Callable` or `list`")

        from jinja2 import pass_context
        @pass_context
        def as_template(context, value):
            tpl = self.env.from_string(value)
            return tpl.render(**context)

        self.env.filters['as_template'] = as_template

        self.markdown = Markdown(extensions=['toc', 'extra'])
        self.ts = Ts()
        self._meta    = DotDict()
        self._sql     = DotDict()
        self._tables  = DotDict()
        self._charts  = DotDict()

        context = {
            'pd': pd,
            'ts': self.ts,
            'meta': self.meta,
            'sql': self.sql,
            'tables': self.tables,
            'charts': self.charts,
        }
        self.env.globals |= context
        self.load_config()

    @property
    def meta(self) -> DotDict:
        return self._meta

    @meta.setter
    def meta(self, value) -> None:
        raise AttributeError("Cannot overwrite namespace.")

    @property
    def sql(self) -> DotDict:
        return self._sql

    @sql.setter
    def sql(self, value) -> None:
        raise AttributeError("Cannot overwrite namespace.")

    @property
    def tables(self) -> DotDict:
        return self._tables

    @tables.setter
    def tables(self, value) -> None:
        raise AttributeError("Cannot overwrite namespace.")

    @property
    def charts(self) -> DotDict:
        return self._charts

    @charts.setter
    def charts(self, value) -> None:
        raise AttributeError("Cannot overwrite namespace.")

    def render_to_layout(
        self,
        path_to_markdown: str|Path,
        path_to_template: str = 'report.html',
        post = None,
        **kwargs
    ) -> str:
        if post is None:
            pp = self.post.copy()
        elif post == False:
            pp = []
        elif isinstance(post, Callable):
            pp = [*self.post, post]
        elif isinstance(post, list):
            pp = [*self.post, *post]

        tpl = self.env.get_template(path_to_template)
        content = self.render_content_as_raw_html(path_to_markdown, **kwargs)
        for postprocessor in pp:
            content = postprocessor(content)
        as_html = tpl.render(
            content=content,
            toc=self.markdown.toc_tokens,
            **kwargs
        )
        return as_html

    def render_content_as_md(
        self,
        path_to_markdown: str|Path,
        **kwargs
    ) -> str:
        tpl = self.env.get_template(path_to_markdown)
        as_markdown = tpl.render(**kwargs)
        return as_markdown

    def render_content_as_raw_html(
        self,
        path_to_markdown: str|Path,
        **kwargs
    ) -> str:
        as_markdown = self.render_content_as_md(path_to_markdown, **kwargs)
        as_html = self.markdown.convert(as_markdown)
        return as_html

    def to_html(
        self,
        source: str|Path,
        output_path: str|Path,
        path_to_template: str|Path = 'report.html',
        post = None,
        **kwargs
    ) -> None:
        as_html = self.render_to_layout(
            source,
            path_to_template,
            post=post,
            **kwargs
        )
        Path(output_path).write_text(as_html)

    def to_docx(
        self,
        source: str|Path,
        output_path: str|Path,
        path_to_template: str|Path = 'report.html',
        post = None,
        **kwargs
    ) -> None:
        import subprocess
        output_path = Path(output_path).resolve()
        temp_path = output_path.with_suffix('.temp.html')

        as_html = self.render_to_layout(
            source,
            path_to_template,
            post=post,
            **kwargs
        )
        temp_path.write_text(as_html)
        args = ['pandoc', str(temp_path), '-o', str(output_path)]

        r = subprocess.run(args, capture_output=True)
        if error := r.stderr.decode():
            print(error)
        temp_path.unlink()

    @classmethod
    def load_config(cls) -> None:
        conf = json.loads((cls.TEMPLATES / 'config.json').read_text())
        for pat, val in conf.items():
            pd.set_option(pat, val)
