import setuptools

# A bit of pbr monkey-patching
# We'd like to have a way to compose extras.
import pbr.util
orig_setup_cfg_to_setup_kwargs = pbr.util.setup_cfg_to_setup_kwargs

def lookup_extras(requirement, extras):
    if not requirement.startswith('['):
        return [requirement]
    extra = requirement.strip('[]')
    requirements = []
    for requirement in extras[extra]:
        requirements += lookup_extras(requirement, extras)
    return requirements

def setup_cfg_to_setup_kwargs(*args, **kwargs):
    kwargs = orig_setup_cfg_to_setup_kwargs(*args, **kwargs)
    extras = kwargs['extras_require']
    for extra, requirements in extras.items():
        new_requirements = []
        for requirement in requirements:
            new_requirements += lookup_extras(requirement, extras)
        extras[extra] = new_requirements
    return kwargs

pbr.util.setup_cfg_to_setup_kwargs = setup_cfg_to_setup_kwargs


setuptools.setup(
    setup_requires=['pbr'],
    pbr=True,
)
