from . import step
from buelon.helpers import pipe_util
from . import pipe_interpreter


class StepDefinition:
    name: str
    language: str
    function: str
    path: str
    code: str
    using_code: bool = False

    priority: int = 0
    velocity: float = None
    tag: str = None
    scope: str = 'default'

    def __init__(self, name: str, language: str, function: str, path: str, code: str = '', using_code: bool = False, scope: str = None):
        self.name = name
        self.language = language
        self.function = function
        self.path = path
        self.code = code
        self.using_code = using_code
        if scope:
            self.scope = scope

    def to_step(self, env=None) -> step.Step:
        _step = step.Step()
        _step.id = pipe_util.get_id()
        _step.name = self.name
        _step.type = self.language

        if self.using_code:
            _step.code = self.code
        else:
            _step.local = True
            _step.code = self.path
        _step.func = self.function
        _step.kwargs = env or {}
        _step.priority = self.priority
        _step.velocity = self.velocity
        _step.tag = self.tag
        _step.scope = self.scope
        return _step


def check_for_step_definition(index: int, line: str, variables: dict, configurations: dict) -> bool:
    i = index
    if configurations['importing']:
        importing_value: StepDefinition = variables[configurations['current import']]

        if line.strip().startswith('!'):
            if line.count('!') > 1:
                raise SyntaxError(f'Line {i + 1}: Invalid block')
            try:
                importing_value.priority = int(line.strip()[1:])
                return True
            except ValueError:
                raise SyntaxError(f'Line {i + 1}: Invalid priority. Must be integer.')

        if line.strip().startswith('$') and line.strip().count('$') == 1:
            if line.count('$') > 1:
                raise SyntaxError(f'Line {i + 1}: Invalid block')
            importing_value.scope = pipe_util.trim(line.strip()[1:])
            return True

        if line.strip().startswith('@') and line.strip().count('@') == 1:
            if line.count('@') > 1:
                raise SyntaxError(f'Line {i + 1}: Invalid block')
            _line = line.strip()[1:].strip()
            if not _line:
                raise SyntaxError(f'Line {i + 1}: Tag/Velocity cannot be empty')
            if not (set(_line) - set('*/+-. 1234567890')):
                try:
                    importing_value.velocity = eval(_line)
                    return True
                except Exception as e:
                    raise SyntaxError(f'Line {i + 1}: Velocity could not be calculated. ({e})')
            _line = pipe_util.trim(_line)
            if _line not in variables:
                raise SyntaxError(f'Line {i + 1}: Tag/Velocity not found')
            importing_value.tag = pipe_util.trim(_line)
            importing_value.velocity = variables[_line]
            return True

        if not importing_value.language:
            importing_value.language = line.strip()
            if importing_value.language in pipe_interpreter.LANGUAGES:
                importing_value.language = pipe_interpreter.LANGUAGES[importing_value.language]
                return True
            else:
                languages = ', '.join(pipe_interpreter.LANGUAGES.keys())
                raise SyntaxError(
                    f'line {i + 1}: Language {importing_value.language} not found. Languages: {languages}')
        if not importing_value.function:
            importing_value.function = line.strip()
            if not importing_value.function:
                raise SyntaxError(f'line {i + 1}: Function cannot be empty.')
            return True
        if not importing_value.path and not importing_value.using_code:
            def strip_for_block(line):
                return line.strip()  # .replace('\\`', '')

            if not configurations['in code block']:
                if strip_for_block(line).startswith('`'):
                    if strip_for_block(line).endswith('`') and strip_for_block(line).count('`') == 2:
                        importing_value.code = line.strip()[1:-1]
                        importing_value.using_code = True
                        return True
                    elif strip_for_block(line).count('`') > 2:
                        raise SyntaxError(f'Line {i + 1}: Invalid block')
                    importing_value.code = line.strip()[1:]
                    configurations['in code block'] = True
                    return True
                else:
                    importing_value.path = pipe_util.trim(line)
                    return True
            else:
                if strip_for_block(line).endswith('`'):
                    if strip_for_block(line).count('`') > 1:
                        raise SyntaxError(f'Line {i + 1}: Invalid block')
                    importing_value.code += '\n' + line[:line.index('`')]
                    configurations['in code block'] = False
                    importing_value.using_code = True
                    return True
                else:
                    if strip_for_block(line).count('`') > 0:
                        raise SyntaxError(f'Line {i + 1}: Invalid block')
                    importing_value.code += '\n' + line
                    return True
        configurations['importing'] = False

    token = pipe_interpreter.PIPE_TOKENS['import']
    if line.strip().endswith(token) and not line.strip().startswith('for'):
        line = pipe_util.trim(line[:-len(token)])
        variables[line] = StepDefinition(line, '', '', '', '', scope=variables['__scope__'])
        configurations['current import'] = line
        configurations['importing'] = True
        return True
    return False
