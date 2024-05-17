# -*- coding: utf-8 -*-

# python3 shims
try:
    basestring  # noqa: B018
except NameError:
    basestring = str


_CONTEXT_KEYS_TO_CLEAN = (
    "group_by",
    "pivot_measures",
    "pivot_column_groupby",
    "pivot_row_groupby",
    "graph_groupbys",
    "orderedBy",
)

def clean_context(context, fieldname):
    """Remove (in place) all references to the field in the context dictionary."""

    def filter_value(key, value):
        if key == "orderedBy" and isinstance(value, dict):
            res = {k: (filter_value(None, v) if k == "name" else v) for k, v in value.items()}
            # return if name didn't match fieldname
            return res if "name" not in res or res["name"] is not None else None
        if not isinstance(value, basestring):
            # if not a string, ignore it
            return value
        if value.split(":")[0] != fieldname:
            # only return if not matching fieldname
            return value
        return None  # value filtered out

    if not isinstance(context, dict):
            return False

    changed = False
    for key in _CONTEXT_KEYS_TO_CLEAN:
        if context.get(key):
            context_part = [filter_value(key, e) for e in context[key]]
            changed |= context_part != context[key]
            context[key] = [e for e in context_part if e is not None]

    for vt in ["pivot", "graph", "cohort"]:
        key = "{}_measure".format(vt)
        if key in context:
            new_value = filter_value(key, context[key])
            changed |= context[key] != new_value
            context[key] = new_value if new_value is not None else "id"

        if vt in context:
            changed |= clean_context(context[vt])

    return changed

def adapt_context(context, old, new):
    """Replace (in place) all references to field `old` to `new` in the context dictionary."""

    # adapt (in place) dictionary values
    if not isinstance(context, dict):
        return

    for key in _CONTEXT_KEYS_TO_CLEAN:
        if context.get(key):
            context[key] = [_adapt_context_value(key, e, old, new) for e in context[key]]

    for vt in ["pivot", "graph", "cohort"]:
        key = "{}_measure".format(vt)
        if key in context:
            context[key] = _adapt_context_value(key, context[key], old, new)

        if vt in context:
            adapt_context(context[vt], old, new)

    def_old = "default_{}".format(old)
    def_new = "default_{}".format(new)

    if def_old in context:
        context[def_new] = context.pop(def_old)


def _adapt_context_value(key, value, old, new):
    if key == "orderedBy" and isinstance(value, dict):
        # only adapt the "name" key
        return {k: (_adapt_context_value(None, v, old, new) if k == "name" else v) for k, v in value.items()}

    if not isinstance(value, basestring):
        # ignore if not a string
        return value

    parts = value.split(":", 1)
    if parts[0] != old:
        # if not match old, leave it
        return value
    # change to new, and return it
    parts[0] = new
    return ":".join(parts)
