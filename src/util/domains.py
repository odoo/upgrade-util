# -*- coding: utf-8 -*-
import collections
import logging

try:
    from odoo.osv import expression
    from odoo.tools import ustr
    from odoo.tools.safe_eval import safe_eval
except ImportError:
    from openerp.osv import expression
    from openerp.tools import ustr
    from openerp.tools.safe_eval import safe_eval

from .const import NEARLYWARN
from .helpers import _dashboard_actions, _validate_model
from .inherit import for_each_inherit
from .misc import SelfPrintEvalContext
from .pg import column_exists, table_exists

# python3 shims
try:
    basestring
except NameError:
    basestring = unicode = str

_logger = logging.getLogger(__name__)
DomainField = collections.namedtuple("DomainField", "table domain_column model_select")


def _get_domain_fields(cr):
    # haaa, if only we had a `fields.Domain`, we would just have to get all the domains from `ir_model_fields`
    # Meanwile, we have to enumerate them explicitly
    # false friends: the `domain` fields on `website` and `amazon.marketplace` are actually domain names.
    # NOTE: domains on transient models have been ignored
    mmm = []
    if column_exists(cr, "mail_mass_mailing", "mailing_model_id"):
        # >= saas~18
        mmm = [
            DomainField(
                "mail_mass_mailing", "mailing_domain", "(SELECT model FROM ir_model m WHERE m.id = t.mailing_model_id)"
            )
        ]
    elif column_exists(cr, "mail_mass_mailing", "mailing_model"):
        # >= saas~4
        mmm = [DomainField("mail_mass_mailing", "mailing_domain", "mailing_model")]
    else:
        mail_template = "mail_template" if table_exists(cr, "mail_template") else "email_template"
        mmm = [
            DomainField(
                "mail_mass_mailing",
                "mailing_domain",
                "(SELECT model FROM {} m WHERE m.id = t.template_id)".format(mail_template),
            )
        ]

    documents_domains_target = "'documents.document'" if table_exists(cr, "documents_document") else "'ir.attachment'"

    result = mmm + [
        DomainField("ir_model_fields", "domain", "model"),
        DomainField("ir_act_window", "domain", "res_model"),
        DomainField("ir_filters", "domain", "model_id"),  # model_id is a varchar
        DomainField("ir_rule", "domain_force", "(SELECT model FROM ir_model m WHERE m.id = t.model_id)"),
        DomainField("document_directory", "domain", "(SELECT model FROM ir_model m WHERE m.id = t.ressource_type_id)"),
        DomainField(
            "mailing_mailing", "mailing_domain", "(SELECT model FROM ir_model m WHERE m.id = t.mailing_model_id)"
        ),
        DomainField("base_action_rule", "filter_domain", "(SELECT model FROM ir_model m WHERE m.id = t.model_id)"),
        DomainField("base_action_rule", "filter_pre_domain", "(SELECT model FROM ir_model m WHERE m.id = t.model_id)"),
        DomainField(
            "base_automation", "filter_domain", "(SELECT model_name FROM ir_act_server WHERE id = t.action_server_id)"
        ),
        DomainField(
            "base_automation",
            "filter_pre_domain",
            "(SELECT model_name FROM ir_act_server WHERE id = t.action_server_id)",
        ),
        DomainField("gamification_goal_definition", "domain", "(SELECT model FROM ir_model m WHERE m.id = t.model_id)"),
        DomainField("marketing_campaign", "domain", "model_name"),
        DomainField(
            "marketing_activity", "domain", "(SELECT model_name FROM marketing_campaign WHERE id = t.campaign_id)"
        ),
        DomainField(
            "marketing_activity",
            "activity_domain",
            "(SELECT model_name FROM marketing_campaign WHERE id = t.campaign_id)",
        ),
        DomainField("data_merge_model", "domain", "res_model_name"),
        # static target model
        DomainField("account_financial_html_report_line", "domain", "'account.move.line'"),
        DomainField("gamification_challenge", "user_domain", "'res.users'"),
        DomainField("pos_cache", "product_domain", "'product.product'"),
        DomainField("sale_coupon_rule", "rule_partners_domain", "'res.partner'"),
        DomainField("sale_coupon_rule", "rule_products_domain", "'product.product'"),
        DomainField("sale_subscription_template", "good_health_domain", "'sale.subscription'"),
        DomainField("sale_subscription_template", "bad_health_domain", "'sale.subscription'"),
        DomainField("website_crm_score", "domain", "'crm.lead'"),
        DomainField("team_user", "team_user_domain", "'crm.lead'"),
        DomainField("crm_team", "score_team_domain", "'crm.lead'"),
        DomainField("social_post", "visitor_domain", "'website.visitor'"),
        DomainField("documents_share", "domain", documents_domains_target),
        DomainField("documents_workflow_rule", "domain", documents_domains_target),
        DomainField("loyalty_rule", "rule_domain", "'product.product'"),
    ]

    for df in result:
        if column_exists(cr, df.table, df.domain_column):
            yield df


def _adapt_one_domain(cr, target_model, old, new, model, domain, adapter=None, force_adapt=False):
    if not adapter:
        adapter = lambda leaf, _, __: [leaf]

    evaluation_context = SelfPrintEvalContext()

    # pre-check domain
    if isinstance(domain, basestring):
        try:
            eval_dom = expression.normalize_domain(safe_eval(domain, evaluation_context, nocopy=True))
        except Exception as e:
            oops = ustr(e)
            _logger.log(NEARLYWARN, "Cannot evaluate %r domain: %r: %s", model, domain, oops)
            return None
    else:
        try:
            eval_dom = expression.normalize_domain(domain)
        except Exception as e:
            oops = ustr(e)
            _logger.log(NEARLYWARN, "Invalid %r domain: %r: %s", model, domain, oops)
            return None

    def valid_path_to(cr, path, from_, to):
        model = from_
        while path:
            field = path.pop(0)
            cr.execute(
                """
                SELECT relation
                  FROM ir_model_fields
                 WHERE model = %s
                   AND name = %s
            """,
                [model, field],
            )
            if not cr.rowcount:
                # unknown field. Maybe an old domain. Cannot validate it.
                return False
            [model] = cr.fetchone()

        return model == to

    def clean_path(left):
        path = left.split(".")
        for idx in range(1, len(path) + 1):
            if path[-idx] == old and valid_path_to(cr, path[:-idx], model, target_model):
                path[-idx] = new
        return ".".join(path)

    def clean_term(term):
        if isinstance(term, basestring) or not isinstance(term[0], basestring):
            return term
        return (clean_path(term[0]), term[1], term[2])

    final_dom = []
    changed = False
    op_arity = {expression.NOT_OPERATOR: 1, expression.AND_OPERATOR: 2, expression.OR_OPERATOR: 2}
    op_stack = []  # (operator, number of terms missing)
    for element in eval_dom:
        while op_stack and op_stack[-1][1] == 0:
            op_stack.pop()  # found all terms current operator was expecting, pop it
            op_stack[-1][1] -= 1  # previous operator now got one more term

        if isinstance(element, basestring):
            if element not in op_arity:
                _logger.log(NEARLYWARN, "Invalid domain on %r: %s", model, domain)
                return None
            op_stack.append([element, op_arity[element]])
            final_dom.append(element)
            continue

        if not expression.is_leaf(element):
            _logger.log(NEARLYWARN, "Invalid domain on %r: %s", model, domain)
            return None

        if op_stack:
            op_stack[-1][1] -= 1  # previous operator got a term

        if tuple(element) in [expression.TRUE_LEAF, expression.FALSE_LEAF]:
            final_dom.append(element)
            continue

        is_or = False
        neg = False
        for op, _ in reversed(op_stack):
            if op != expression.NOT_OPERATOR:
                is_or = op == expression.OR_OPERATOR
                break
            neg = not neg

        leaf = expression.normalize_leaf(element)
        path = leaf[0].split(".")
        # force_adapt=True -> always adapt if found anywhere on left path
        # otherwise adapt only when {old} field is the last part of left path
        search_range = range(len(path)) if force_adapt else [-1]
        if any(path[i] == old and valid_path_to(cr, path[:i], model, target_model) for i in search_range):
            dom = [clean_term(term) for term in adapter(leaf, is_or, neg)]
        else:
            dom = [clean_term(leaf)]

        if dom != [leaf]:
            changed = True

        final_dom.extend(dom)

    if not changed:
        return None

    _logger.debug("%s: %r -> %r", model, domain, final_dom)
    return final_dom


def adapt_domains(cr, model, old, new, adapter=None, skip_inherit=(), force_adapt=False):
    """
    Replace {old} by {new} in all domains for model {model} using an adapter callback.

    {adapter} is to adapt leafs. It is a function that takes three arguments and
    returns a domain that substitutes the original leaf:
    (leaf: Tuple[str,str,Any], in_or: bool, negated: bool) -> List[Union[str,Tuple[str,str,Any]]]

    The parameter {in_or} signals that the leaf is part of an or ("|") domain, otherwise
    it is part of an and ("&") domain. The other parameter signals if the leaf is
    {negated} ("!").

    Note that the {adapter} is called ony on leafs that use the {old} field of {model}.

    {force_adapt} will run the adapter on all leaves having the removed field in the path. Useful
    when deleting a field (in which case {new} is ignored).
    """
    _validate_model(model)
    target_model = model

    match_old = r"\y{}\y".format(old)
    for df in _get_domain_fields(cr):
        cr.execute(
            """
            SELECT id, {df.model_select}, {df.domain_column}
              FROM {df.table} t
             WHERE {df.domain_column} ~ %s
        """.format(
                df=df
            ),
            [match_old],
        )
        for id_, model, domain in cr.fetchall():
            domain = _adapt_one_domain(
                cr, target_model, old, new, model, domain, adapter=adapter, force_adapt=force_adapt
            )
            if domain:
                cr.execute(
                    "UPDATE {df.table} SET {df.domain_column} = %s WHERE id = %s".format(df=df), [unicode(domain), id_]
                )

    # adapt domain in dashboards.
    # NOTE: does not filter on model at dashboard selection for handle dotted domains
    for _, act in _dashboard_actions(cr, match_old):
        if act.get("domain"):
            try:
                act_id = int(act.get("name", "FAIL"))
            except ValueError:
                continue

            cr.execute("SELECT res_model FROM ir_act_window WHERE id = %s", [act_id])
            if not cr.rowcount:
                continue
            [act_model] = cr.fetchone()
            domain = _adapt_one_domain(
                cr, target_model, old, new, act_model, act.get("domain"), adapter=adapter, force_adapt=force_adapt
            )
            if domain:
                act.set("domain", unicode(domain))

    # down on inherits
    for inh in for_each_inherit(cr, target_model, skip_inherit):
        adapt_domains(cr, inh.model, old, new, adapter, skip_inherit=skip_inherit, force_adapt=force_adapt)
