"""Helper script to generate methods from xml spec."""
import os
from datetime import datetime
from xml.etree import ElementTree
from collections import OrderedDict


def load_spec(filename):
    tree = ElementTree.parse(filename)
    methods = get_methods(tree)
    error_classes = get_error_classes(tree)
    return methods, error_classes


def get_methods(tree):
    classes = get_classes(tree)
    methods = []
    for cname, (cid, cmethods) in classes.items():
        for mname, (mid, mfields, msupport, synchronous, content, mdoc) in cmethods.items():  # noqa
            name = cname + mname
            methods.append((name, mdoc, (cid, mid), mfields, synchronous, content))
    return methods


def get_error_classes(tree):
    classes = []
    for elem in tree.findall('constant'):
        kind = elem.attrib.get('class')
        if kind is None:
            continue
        name = elem.attrib['name']
        value = elem.attrib['value']
        doc = build_docstring(elem)
        # Convert name and kind from foo-bar to FooBar
        name = ''.join(map(str.capitalize, name.split('-')))
        kind = ''.join(map(str.capitalize, kind.split('-')))

        classes.append((name, value, kind, doc))

    return classes


def get_classes(tree):
    domain_types = {e.attrib['name']: e.attrib['type']
                    for e in tree.findall('domain')}
    classes = OrderedDict()
    for class_elem in tree.findall('class'):
        class_methods = OrderedDict()
        for method_elem in class_elem.findall('method'):

            fields = OrderedDict()
            for field_elem in method_elem.findall('field'):
                field_name = field_elem.attrib['name'].replace('-', '_')
                try:
                    field_type = domain_types[field_elem.attrib['domain']]
                except KeyError:
                    field_type = field_elem.attrib['type']
                fields[field_name] = field_type.capitalize()

            method_support = OrderedDict()
            for elem in method_elem.findall('chassis'):
                method_support[elem.attrib['name']] = elem.attrib['implement']

            doc = build_docstring(method_elem, fields)
            synchronous = 'synchronous' in method_elem.attrib
            content = 'content' in method_elem.attrib

            method_id = int(method_elem.attrib['index'])
            method_name = (method_elem.attrib['name']
                           .capitalize()
                           .replace('-ok', 'OK')
                           .replace('-empty', 'Empty')
                           .replace('-async', 'Async'))
            class_methods[method_name] = (method_id, fields, method_support,
                                          synchronous, content, doc)

        class_id = int(class_elem.attrib['index'])
        classes[class_elem.attrib['name'].capitalize()] = (class_id,
                                                           class_methods)
    return classes


def build_docstring(elem, fields=None):
    text = elem.find('doc').text.split()
    doc = []
    line = []
    first = True
    for word in text:
        if len(' '.join(line)) + len(word) > (70 if first else 74):
            first = False
            doc.append(' '.join(line))
            line = []
        line.append(word)
    if line:
        doc.append(' '.join(line))
    description = '\n    '.join(doc)
    if fields is None:
        return description

    args_from_fields = ['    {}: {}'.format(n, t) for n, t in fields.items()]
    if args_from_fields:
        args = '\n\n    Arguments:\n    {}'.format(
            '\n    '.join(args_from_fields)
        )
    else:
        args = ''
    return ''.join((description, args))


if __name__ == '__main__':
    import jinja2
    proj_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spec = '/codegen/amqp0-9-1.extended.xml'
    spec_source = proj_dir + spec
    methods_file = proj_dir + '/amqproto/protocol/methods.py'
    methods_template_source = proj_dir + '/codegen/methods.py.tmpl'
    errors_file = proj_dir + '/amqproto/protocol/errors.py'
    errors_template_source = proj_dir + '/codegen/errors.py.tmpl'
    methods, error_classes = load_spec(spec_source)

    env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)

    gendate = datetime.utcnow().strftime('%Y-%m-%d')

    with open(methods_template_source) as f:
        template = env.from_string(f.read())
    rendered = template.render(
        gendate=gendate,
        gensource=spec,
        methods=methods)
    with open(methods_file, 'w') as f:
        f.write(rendered)

    with open(errors_template_source) as f:
        template = env.from_string(f.read())
    rendered = template.render(
        gendate=gendate,
        gensource=spec,
        error_classes=error_classes)
    with open(errors_file, 'w') as f:
        f.write(rendered)
