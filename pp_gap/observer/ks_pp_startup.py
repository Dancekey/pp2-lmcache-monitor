"""Explicit opt-in PP2 bootstrap; import after the existing source finder."""
import os

if os.environ.get('KS_PP_DIAG') == '1' and os.environ.get('KS_PP_DIAG_PP') == '2':
    import importlib.abc
    import importlib.machinery
    import sys
    import ks_pp_trace

    class Loader(importlib.abc.Loader):
        def __init__(self, original, name):
            self.original, self.name = original, name

        def create_module(self, spec):
            return self.original.create_module(spec)

        def exec_module(self, module):
            # Custom loaders may contain essential production transformations.
            # Preserve their exec path; never bypass them by recompiling source.
            if not isinstance(self.original, importlib.machinery.SourceFileLoader):
                self.original.exec_module(module)
                ks_pp_trace.emit('hook.coverage', module=self.name, source=getattr(module,'__file__',None),
                                 sites=[], gap='NONSTANDARD_LOADER_STATEMENT_HOOKS_NOT_INSTALLED')
            else:
                source = self.original.get_source(self.name)
                tree, sites = ks_pp_trace.transform(source, module.__file__, self.name)
                exec(compile(tree, module.__file__, 'exec'), module.__dict__)
                ks_pp_trace.emit('hook.coverage', module=self.name, source=module.__file__, sites=sites)
            ks_pp_trace.install(module)

    class Finder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname not in ks_pp_trace.TARGETS:
                return None
            spec = None
            # Ask the original finder chain in its original order.
            for finder in sys.meta_path[sys.meta_path.index(self)+1:]:
                find = getattr(finder,'find_spec',None)
                if find is not None:
                    spec = find(fullname,path,target)
                    if spec is not None:
                        break
            if spec is not None and spec.loader is not None:
                spec.loader = Loader(spec.loader, fullname)
            return spec

    sys.meta_path.insert(0, Finder())
    ks_pp_trace.emit('observer.enabled', already_loaded=sorted(set(sys.modules) & ks_pp_trace.TARGETS))
