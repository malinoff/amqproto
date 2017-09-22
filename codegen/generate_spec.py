"""Helper script to generate methods from xml spec."""
import os
from xml.etree import ElementTree
from collections import OrderedDict


def load_spec(filename):
    tree = ElementTree.parse(filename)
    constants = constants_from(tree)
    replies = replies_from(tree)
    domains = domains_from(tree)
    classes = classes_from(tree)
    return constants, replies, domains, classes


def constants_from(tree):
    constants = OrderedDict()
    for elem in tree.findall('constant'):
        name = elem.attrib['name']
        value = elem.attrib['value']
        # Sanitize name
        name = '_'.join(map(str.upper, name.split('-')))
        constants[name] = value
    return constants


def sanitize_doc(doc):
    if doc is None:
        return ''
    if doc.attrib.get('type') == 'grammar':
        return doc.text.strip()
    return ' '.join(map(str.strip, doc.text.strip().splitlines()))


def to_camelcase(value):
    return ''.join(map(str.capitalize, value.split('-')))


def replies_from(tree):
    replies = OrderedDict()
    for elem in tree.findall('reply'):
        name = elem.attrib['name']
        if name == 'reply-success':
            # This reply code isn't used
            continue
        klass = elem.attrib['class']
        value = elem.attrib['value']
        # Sanitize name and klass
        name = to_camelcase(name)
        klass = to_camelcase(klass)
        doc = sanitize_doc(elem.find('doc'))
        replies[name] = {
            'value': value,
            'class': klass,
            'doc': doc,
        }
    return replies


def make_assertion(elem):
    return {
        'check': elem.attrib['check'],
        'value': elem.attrib.get('value'),
    }


def domains_from(tree):
    domains = OrderedDict()
    for elem in tree.findall('domain'):
        name = elem.attrib['name']
        type = elem.attrib['type']
        # ignore primitive domains
        if name == type:
            continue
        name = to_camelcase(name)
        type = type.capitalize()
        label = elem.attrib['label'].capitalize() + '.'
        doc = sanitize_doc(elem.find('doc'))
        rules = OrderedDict()
        for rule in elem.findall('rule'):
            r_name = rule.attrib['name'].strip()
            r_doc = sanitize_doc(rule.find('doc')).strip()
            rules[r_name] = r_doc
        assertions = []
        for assertion in elem.findall('assert'):
            assertions.append(make_assertion(assertion))
        domains[name] = {
            'type': type,
            'label': label,
            'doc': doc,
            'rules': rules,
            'assertions': assertions,
        }
    return domains


def classes_from(tree):
    classes = OrderedDict()
    for elem in tree.findall('class'):
        name = elem.attrib['name']
        handler = elem.attrib['handler']
        index = elem.attrib['index']
        label = elem.attrib['label'].capitalize() + '.'
        docs = []
        for doc in elem.findall('doc'):
            docs.append((doc.attrib.get('type'), sanitize_doc(doc)))
        methods = methods_from(elem)
        classes[name.capitalize()] = {
            'index': index,
            'label': label,
            'docs': docs,
            'methods': methods,
        }
        if name != handler:
            classes[name.capitalize()]['handler'] = handler
    return classes


def methods_from(tree):
    methods = OrderedDict()
    for elem in tree.findall('method'):
        name = elem.attrib['name'].replace('-', '_')
        if name == 'return':
            name = 'return_'
        synchronous = 'synchronous' in elem.attrib
        index = elem.attrib['index']
        label = elem.attrib.get('label')
        if label is not None:
            label = label.capitalize() + '.'
        doc = sanitize_doc(elem.find('doc'))
        rules = OrderedDict()
        for rule in elem.findall('rule'):
            r_name = rule.attrib['name']
            rules[r_name] = sanitize_doc(rule.find('doc'))
        response = elem.find('response')
        if response is not None:
            response = response.attrib['name'].replace('-', '_')
        fields = fields_from(elem)
        non_reserved = OrderedDict()
        for field_name, field_opts in fields.items():
            if not field_opts['reserved']:
                non_reserved[field_name] = field_opts
        method = {
            'synchronous': synchronous,
            'index': index,
            'label': label,
            'doc': doc,
            'rules': rules,
            'response': response,
            'fields': fields,
            'non_reserved': non_reserved,
            'all_reserved': all(field['reserved']
                                for field in fields.values()),
        }
        for chassis in elem.findall('chassis'):
            if chassis.attrib['name'] == 'server':
                methods[name] = method
            else:  # chassis.attrib['name'] == 'client'
                methods['on_' + name] = method
    return methods


def fields_from(tree):
    fields = OrderedDict()
    for field in tree.findall('field'):
        name = field.attrib['name'].replace('-', '_')
        if name == 'global':
            name = 'global_'
        domain = to_camelcase(field.attrib['domain'])
        reserved = 'reserved' in field.attrib
        label = field.attrib.get('label')
        if label is not None:
            label = label.capitalize() + '.'
        doc = sanitize_doc(field.find('doc'))
        rules = OrderedDict()
        for rule in field.findall('rule'):
            r_name = rule.attrib['name']
            on_failure = rule.attrib.get('on-failure')
            if on_failure is not None:
                on_failure = to_camelcase(on_failure)
            rules[r_name] = {
                'on_failure': on_failure,
                'doc': sanitize_doc(rule.find('doc')),
            }
        assertions = []
        for assertion in field.findall('assert'):
            assertions.append(make_assertion(assertion))
        field = {
            'domain': domain,
            'reserved': reserved,
            'label': label,
            'doc': doc,
            'rules': rules,
            'assertions': assertions,
        }
        fields[name] = field
    return fields


if __name__ == '__main__':
    import jinja2
    proj_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spec = '/codegen/amqp0-9-1.extended.xml'
    spec_source = proj_dir + spec
    constants, replies, domains, classes = load_spec(spec_source)

    env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)

    for tmpl in os.listdir(proj_dir + '/codegen/templates'):
        with open(proj_dir + '/codegen/templates/' + tmpl) as f:
            template = env.from_string(f.read())

        rendered = template.render(
            constants=constants,
            domains=domains,
            replies=replies,
            classes=classes,
        )

        with open(proj_dir + '/amqproto/protocol/{}.py'.format(tmpl[:-3]), 'w') as f:
            f.write(rendered)
