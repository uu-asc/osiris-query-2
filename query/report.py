from pathlib import Path
from typing import Callable, NewType

from jinja2 import Environment, FileSystemLoader, pass_context
from markdown import Markdown
import pandas as pd

from query.config import CONFIG, get_paths_from_config
from query.utils import DotDict, TS


markdownStr = NewType('markdownStr', str)
htmlStr = NewType('htmlStr', str)

type PostProcessor = Callable[[str], str]
type PostProcessors = list[Callable[[str], str]]
type OptionalPostProcessors = PostProcessors | None


def render_template(value: str, context: dict) -> str:
    env = Environment()
    template = env.from_string(value)
    rendered = template.render(context)
    return rendered


class Report:
    """
    Class for creating reports from markdown templates.

        Workflow:
        Input
            [markdown template]
            [layout template]

        Step 1: Render markdown as naked html
        Step 2: Postprocess naked html
        Step 3: Render naked html into layout

        Output
            [rendered html]

    The following attributes are exposed to the rendering context:
    - pd: Pandas.
    - ts: several timestamps.
    - meta: container for storing meta information.
    - sql: container for storing sql queries.
    - tables: container for storing tables.
    - charts: container for storing chart specifications.

    Also a `as_template` filter is added to the rendering environment.
    This allows you to do: `{{ snippet | as_template }}` where `snippet`
    will be rendered as a template using the context of the main template.

    Attributes:
    - markdown_template (str): location of markdown template.
    - layout_template (str): location of layout template.
    - post (OptionalPostProcessors): postprocessors.
    - env (Environment): Jinja2 environment for rendering.
    - meta (DotDict): container for storing meta information.
    - sql (DotDict): container for storing sql queries.
    - tables (DotDict): container for storing tables.
    - charts (DotDict): container for storing chart specifications.

    Methods:
    - get_rendered_html() -> htmlStr: Render markdown template to layout.
    - get_naked_html() -> htmlStr: Convert rendered markdown template to html.
    - get_rendered_markdown() -> htmlStr: Render markdown template to markdown.
    - to_html(outpath: Path | str) -> None: Save rendered report as 'html'.
    - to_docx(outpath: Path | str) -> None: Save rendered report as 'docx'.
    - load_config() -> None: Load settings from CONFIG.
    """

    TEMPLATES: list[Path] = get_paths_from_config('templates')

    def __init__(
        self,
        markdown_template: str|None = None,
        layout_template: str|None = None,
        template_paths: Path|str|list[Path|str]|None = None,
        post: OptionalPostProcessors = None,
    ):
        """
        Parameters:
        - markdown_template (str|None, optional):
            Name/location of the markdown template.
        - layout_template (str|None, optional):
            Name/location of the layout template.
        - template_paths (Path|str|list[Path|str]|None, optional):
            Environment paths (used for locating the templates).
            By default CONFIG paths and current working directory are added.
        - post: (OptionalPostProcessors, optional):
            Postprocessor (or list thereof) to be applied to converted markdown.

        Returns:
        None
        """
        self.markdown_template = markdown_template
        self.layout_template = layout_template

        if isinstance(template_paths, (str, Path)):
            template_paths = [template_paths]
        elif template_paths is None:
            template_paths = []

        template_paths = [
            *template_paths,
            '.',
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

        @pass_context
        def as_template(context, value):
            tpl = self.env.from_string(value)
            return tpl.render(**context)

        self.env.filters['as_template'] = as_template

        self.markdown = Markdown(extensions=['toc', 'extra'])
        self.ts = TS
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

    def get_rendered_html(
        self,
        markdown_template: str|None = None,
        layout_template: str|None = None,
        post: OptionalPostProcessors = None,
        **kwargs
    ) -> htmlStr:
        """
        Renders a markdown template to a html string:
        1. the template gets rendered to a markdown string.
        2. the markdown string is converted to html.
        3. the converted string is (optionally) run through post-processing.
        4. the converted html is rendered into the layout template.

        Parameters:
        - markdown_template (str|None, optional):
            Name/location of the markdown template.
            Defaults to the `markdown_template` set at initialization.
        - layout_template (str|None, optional):
            Name/location of the layout template.
            Defaults to the `layout_template` set at initialization.
        - post: (OptionalPostProcessors, optional):
            Postprocessor (or list thereof) to be applied to converted markdown.
        - **kwargs: Additional keyword arguments to pass to the template.

        Returns:
        htmlStr: Rendered html content.
        """
        if markdown_template is None:
            markdown_template = self.markdown_template
        if layout_template is None:
            layout_template = self.layout_template

        if post is None:
            pp = self.post.copy()
        elif post == False:
            pp = []
        elif isinstance(post, Callable):
            pp = [*self.post, post]
        elif isinstance(post, list):
            pp = [*self.post, *post]

        tpl = self.env.get_template(layout_template)
        naked_html = self.get_naked_html(markdown_template, **kwargs)

        for postprocessor in pp:
            content = postprocessor(naked_html)
        html = tpl.render(
            content=content,
            toc=self.markdown.toc_tokens,
            **kwargs
        )
        return html

    def get_naked_html(
        self,
        markdown_template: str|None = None,
        **kwargs
    ) -> htmlStr:
        """
        Renders a markdown template to a html string.
        First, the template gets rendered to a markdown string.
        Then the markdown string is converted to html.

        Parameters:
        - markdown_template (str|None, optional):
            Name/location of the markdown template.
            Defaults to the `markdown_template` set at initialization.
        - **kwargs: Additional keyword arguments to pass to the template.

        Returns:
        htmlStr: Rendered html content.
        """
        if markdown_template is None:
            markdown_template = self.markdown_template

        markdown = self.get_rendered_markdown(markdown_template, **kwargs)
        naked_html = self.markdown.convert(markdown)
        return naked_html

    def get_rendered_markdown(
        self,
        markdown_template: str|None = None,
        **kwargs
    ) -> markdownStr:
        """
        Renders a markdown template to a markdown string.

        Parameters:
        - markdown_template (str|None, optional):
            Name/location of the markdown template.
            Defaults to the `markdown_template` set at initialization.
        - **kwargs: Additional keyword arguments to pass to the template.

        Returns:
        markdownStr: Rendered markdown content.
        """
        if markdown_template is None:
            markdown_template = self.markdown_template

        markdown_template = self.env.get_template(markdown_template)
        rendered_markdown = markdown_template.render(**kwargs)
        return rendered_markdown

    def to_html(
        self,
        output_path: Path|str,
        markdown_template: str|None = None,
        layout_template: str|None = None,
        post: OptionalPostProcessors = None,
        **kwargs
    ) -> None:
        """
        Render report to html.

        Parameters:
        - output_path (Path|str): Filepath to store rendered report.
        - markdown_template (str|None, optional):
            Name/location of the markdown template.
            Defaults to the `markdown_template` set at initialization.
        - layout_template (str|None, optional):
            Name/location of the layout template.
            Defaults to the `layout_template` set at initialization.
        - post: (OptionalPostProcessors, optional):
            Postprocessor (or list thereof) to be applied to converted markdown.
        - **kwargs: Additional keyword arguments to pass to the template.

        Returns:
        None
        """
        html = self.get_rendered_html(
            markdown_template,
            layout_template,
            post = post,
            **kwargs
        )
        Path(output_path).write_text(html)

    def to_docx(
        self,
        output_path: Path|str,
        markdown_template: str|None = None,
        layout_template: str|None = None,
        post: OptionalPostProcessors = None,
        **kwargs
    ) -> None:
        """
        Render report to docx.

        Parameters:
        - output_path (Path|str): Filepath to store rendered report.
        - markdown_template (str|None, optional):
            Name/location of the markdown template.
            Defaults to the `markdown_template` set at initialization.
        - layout_template (str|None, optional):
            Name/location of the layout template.
            Defaults to the `layout_template` set at initialization.
        - post: (OptionalPostProcessors, optional):
            Postprocessor (or list thereof) to be applied to converted markdown.
        - **kwargs: Additional keyword arguments to pass to the template.

        Returns:
        None
        """
        import subprocess
        output_path = Path(output_path).resolve()
        temp_path = output_path.with_suffix('.temp.html')

        html = self.get_rendered_html(
            markdown_template,
            layout_template,
            post = post,
            **kwargs
        )
        temp_path.write_text(html)
        args = ['pandoc', str(temp_path), '-o', str(output_path)]

        r = subprocess.run(args, capture_output=True)
        if error := r.stderr.decode():
            print(error)
        temp_path.unlink()

    @staticmethod
    def load_config() -> None:
        for pat, val in CONFIG['report']['styler'].items():
            pd.set_option(pat, val)
