--
-- PostgreSQL database dump
--

\restrict kQWX82pRXAewyutWJv2vQEXcnZpzhEMVd17fE1JzHxCI0nsejCPyr1xan4zE0Bv

-- Dumped from database version 16.11 (Ubuntu 16.11-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.11 (Ubuntu 16.11-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ai_budget_snapshots; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.ai_budget_snapshots (
    id integer NOT NULL,
    provider character varying(50) NOT NULL,
    snapshot_date date NOT NULL,
    total_budget_usd numeric(12,4),
    remaining_usd numeric(12,4),
    daily_spend_usd numeric(12,4),
    burn_rate_7d_usd numeric(12,4),
    estimated_days_left integer,
    api_response jsonb,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.ai_budget_snapshots OWNER TO buddyliko_user;

--
-- Name: ai_budget_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.ai_budget_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ai_budget_snapshots_id_seq OWNER TO buddyliko_user;

--
-- Name: ai_budget_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.ai_budget_snapshots_id_seq OWNED BY public.ai_budget_snapshots.id;


--
-- Name: ai_provider_balance; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.ai_provider_balance (
    id integer NOT NULL,
    provider character varying(20) NOT NULL,
    checked_at timestamp with time zone DEFAULT now() NOT NULL,
    balance_usd numeric(10,2),
    auto_recharge boolean DEFAULT false,
    recharge_amount numeric(10,2),
    recharge_threshold numeric(10,2),
    source character varying(20) DEFAULT 'scraper'::character varying NOT NULL
);


ALTER TABLE public.ai_provider_balance OWNER TO buddyliko_user;

--
-- Name: ai_provider_balance_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.ai_provider_balance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ai_provider_balance_id_seq OWNER TO buddyliko_user;

--
-- Name: ai_provider_balance_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.ai_provider_balance_id_seq OWNED BY public.ai_provider_balance.id;


--
-- Name: ai_token_usage; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.ai_token_usage (
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    provider character varying(20) NOT NULL,
    model character varying(80) NOT NULL,
    operation character varying(40) NOT NULL,
    user_id character varying(80),
    input_tokens integer DEFAULT 0 NOT NULL,
    output_tokens integer DEFAULT 0 NOT NULL,
    total_tokens integer DEFAULT 0 NOT NULL,
    cost_usd numeric(10,6) DEFAULT 0 NOT NULL,
    http_status integer,
    duration_ms integer,
    month character varying(7) NOT NULL,
    org_id uuid
);


ALTER TABLE public.ai_token_usage OWNER TO buddyliko_user;

--
-- Name: ai_token_usage_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.ai_token_usage_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ai_token_usage_id_seq OWNER TO buddyliko_user;

--
-- Name: ai_token_usage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.ai_token_usage_id_seq OWNED BY public.ai_token_usage.id;


--
-- Name: alert_events; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.alert_events (
    id integer NOT NULL,
    alert_type character varying(80) NOT NULL,
    target_type character varying(20) NOT NULL,
    target_id character varying(255),
    target_name character varying(255),
    message text,
    severity character varying(20) DEFAULT 'warning'::character varying,
    channel character varying(20),
    sent_at timestamp with time zone DEFAULT now(),
    period_key character varying(40)
);


ALTER TABLE public.alert_events OWNER TO buddyliko_user;

--
-- Name: alert_events_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.alert_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.alert_events_id_seq OWNER TO buddyliko_user;

--
-- Name: alert_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.alert_events_id_seq OWNED BY public.alert_events.id;


--
-- Name: alert_rules; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.alert_rules (
    id character varying(36) NOT NULL,
    rule_type character varying(80) NOT NULL,
    enabled boolean DEFAULT true,
    channels jsonb DEFAULT '["in_app"]'::jsonb,
    admin_emails jsonb DEFAULT '[]'::jsonb,
    webhook_url text,
    threshold_pct integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.alert_rules OWNER TO buddyliko_user;

--
-- Name: api_tokens; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.api_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    token_hash character varying(255) NOT NULL,
    token_prefix character varying(20) NOT NULL,
    environment character varying(10) DEFAULT 'live'::character varying NOT NULL,
    created_by integer NOT NULL,
    scopes jsonb DEFAULT '["transform:execute"]'::jsonb NOT NULL,
    rate_limit_rpm integer,
    rate_limit_rph integer,
    rate_limit_rpd integer,
    allowed_ips jsonb,
    partner_id uuid,
    tags jsonb DEFAULT '{}'::jsonb,
    expires_at timestamp with time zone,
    status character varying(20) DEFAULT 'active'::character varying NOT NULL,
    revoked_at timestamp with time zone,
    revoked_by integer,
    revoke_reason text,
    last_used_at timestamp with time zone,
    last_used_ip character varying(45),
    use_count bigint DEFAULT 0,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.api_tokens OWNER TO buddyliko_user;

--
-- Name: audit_logs; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.audit_logs (
    id character varying(36) NOT NULL,
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    user_id character varying(255),
    user_email character varying(255),
    user_role character varying(50),
    action character varying(50) NOT NULL,
    resource_type character varying(100),
    resource_id character varying(255),
    outcome character varying(20) DEFAULT 'SUCCESS'::character varying NOT NULL,
    ip_address character varying(64),
    user_agent character varying(512),
    duration_ms integer,
    file_name character varying(500),
    file_size_bytes bigint,
    input_format character varying(50),
    output_format character varying(50),
    error_message text,
    metadata jsonb DEFAULT '{}'::jsonb,
    input_preview text,
    output_preview text,
    org_id uuid
);


ALTER TABLE public.audit_logs OWNER TO buddyliko_user;

--
-- Name: batch_items; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.batch_items (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    batch_id uuid NOT NULL,
    seq integer NOT NULL,
    input_name character varying(500),
    input_path text,
    input_size bigint DEFAULT 0,
    status character varying(20) DEFAULT 'pending'::character varying,
    output_path text,
    output_size bigint DEFAULT 0,
    error text,
    duration_ms integer,
    cost_eur numeric(10,4) DEFAULT 0,
    started_at timestamp with time zone,
    completed_at timestamp with time zone
);


ALTER TABLE public.batch_items OWNER TO buddyliko_user;

--
-- Name: batch_jobs; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.batch_jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    user_id integer,
    name character varying(255),
    description text,
    template_id uuid,
    operation_type character varying(30) DEFAULT 'transform'::character varying,
    config jsonb DEFAULT '{}'::jsonb,
    status character varying(20) DEFAULT 'pending'::character varying,
    total_items integer DEFAULT 0,
    completed_items integer DEFAULT 0,
    failed_items integer DEFAULT 0,
    progress_pct integer DEFAULT 0,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    error text,
    result_summary jsonb DEFAULT '{}'::jsonb,
    output_zip_path text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.batch_jobs OWNER TO buddyliko_user;

--
-- Name: billing_events; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.billing_events (
    id character varying(36) NOT NULL,
    stripe_event_id character varying(255),
    event_type character varying(100),
    user_id character varying(255),
    payload jsonb,
    processed_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.billing_events OWNER TO buddyliko_user;

--
-- Name: budget_alerts; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.budget_alerts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    month character varying(7) NOT NULL,
    threshold integer NOT NULL,
    spent_eur numeric(10,2),
    budget_eur numeric(10,2),
    pct_used numeric(5,1),
    alert_type character varying(20) DEFAULT 'warning'::character varying,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.budget_alerts OWNER TO buddyliko_user;

--
-- Name: db_connections; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.db_connections (
    id character varying(36) NOT NULL,
    user_id character varying(255) NOT NULL,
    name character varying(255) NOT NULL,
    db_type character varying(50) NOT NULL,
    connection_string_enc text NOT NULL,
    default_schema character varying(255),
    created_at timestamp with time zone DEFAULT now(),
    last_used_at timestamp with time zone,
    last_test_status character varying(50) DEFAULT 'unknown'::character varying,
    last_test_message text,
    metadata jsonb DEFAULT '{}'::jsonb,
    org_id uuid
);


ALTER TABLE public.db_connections OWNER TO buddyliko_user;

--
-- Name: discount_codes; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.discount_codes (
    id character varying(36) NOT NULL,
    code character varying(50) NOT NULL,
    description text,
    discount_type character varying(20) NOT NULL,
    discount_value numeric(10,2) NOT NULL,
    applicable_plans jsonb DEFAULT '[]'::jsonb,
    max_uses integer,
    current_uses integer DEFAULT 0,
    valid_from timestamp with time zone DEFAULT now(),
    valid_until timestamp with time zone,
    stripe_coupon_id character varying(255),
    active boolean DEFAULT true,
    created_by character varying(255),
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.discount_codes OWNER TO buddyliko_user;

--
-- Name: file_permissions; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.file_permissions (
    id character varying(36) NOT NULL,
    file_id character varying(36),
    user_id character varying(255),
    group_id character varying(36),
    can_view boolean DEFAULT true,
    can_download boolean DEFAULT false,
    can_copy boolean DEFAULT false,
    can_edit boolean DEFAULT false,
    can_delete boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.file_permissions OWNER TO buddyliko_user;

--
-- Name: files; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.files (
    id character varying(36) NOT NULL,
    name character varying(500) NOT NULL,
    description text,
    file_type character varying(50) NOT NULL,
    owner_id character varying(255),
    group_id character varying(36),
    is_common boolean DEFAULT false,
    is_public boolean DEFAULT false,
    storage_path character varying(1000),
    file_size bigint DEFAULT 0,
    mime_type character varying(255),
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    org_id uuid
);


ALTER TABLE public.files OWNER TO buddyliko_user;

--
-- Name: group_invitations; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.group_invitations (
    id character varying(36) NOT NULL,
    group_id character varying(36),
    email character varying(255) NOT NULL,
    role character varying(50) DEFAULT 'member'::character varying,
    token character varying(255) NOT NULL,
    invited_by character varying(255) NOT NULL,
    status character varying(20) DEFAULT 'pending'::character varying,
    accepted_by_user_id character varying(255),
    message text,
    created_at timestamp without time zone DEFAULT now(),
    expires_at timestamp without time zone DEFAULT (now() + '30 days'::interval),
    accepted_at timestamp without time zone
);


ALTER TABLE public.group_invitations OWNER TO buddyliko_user;

--
-- Name: group_members; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.group_members (
    id character varying(36) NOT NULL,
    group_id character varying(36),
    user_id character varying(255) NOT NULL,
    role character varying(50) DEFAULT 'member'::character varying,
    invited_by character varying(255),
    joined_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.group_members OWNER TO buddyliko_user;

--
-- Name: groups; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.groups (
    id character varying(36) NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    parent_id character varying(36),
    owner_id character varying(255) NOT NULL,
    settings jsonb DEFAULT '{}'::jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    plan_override character varying(50),
    org_id uuid,
    group_type character varying(30) DEFAULT 'working'::character varying
);


ALTER TABLE public.groups OWNER TO buddyliko_user;

--
-- Name: in_app_notifications; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.in_app_notifications (
    id integer NOT NULL,
    user_id character varying(255),
    title character varying(255) NOT NULL,
    message text,
    severity character varying(20) DEFAULT 'warning'::character varying,
    alert_type character varying(80),
    read_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.in_app_notifications OWNER TO buddyliko_user;

--
-- Name: in_app_notifications_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.in_app_notifications_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.in_app_notifications_id_seq OWNER TO buddyliko_user;

--
-- Name: in_app_notifications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.in_app_notifications_id_seq OWNED BY public.in_app_notifications.id;


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.jobs (
    id character varying(36) NOT NULL,
    job_type character varying(50) NOT NULL,
    status character varying(20) DEFAULT 'PENDING'::character varying NOT NULL,
    progress integer DEFAULT 0,
    user_id character varying(255),
    user_email character varying(255),
    created_at timestamp with time zone DEFAULT now(),
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    input_params jsonb DEFAULT '{}'::jsonb,
    input_file_path text,
    input_file_name text,
    input_size_bytes bigint,
    result jsonb,
    output_file_path text,
    output_file_name text,
    output_size_bytes bigint,
    error_message text,
    error_traceback text,
    metadata jsonb DEFAULT '{}'::jsonb,
    org_id uuid,
    environment character varying(10) DEFAULT 'live'::character varying
);


ALTER TABLE public.jobs OWNER TO buddyliko_user;

--
-- Name: mapping_templates; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.mapping_templates (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    author_org_id uuid,
    author_user_id integer,
    name character varying(255) NOT NULL,
    slug character varying(100) NOT NULL,
    description text,
    long_description text,
    category character varying(50) DEFAULT 'other'::character varying NOT NULL,
    input_standard character varying(80),
    output_standard character varying(80),
    input_format character varying(20),
    output_format character varying(20),
    mapping_data jsonb DEFAULT '{}'::jsonb NOT NULL,
    sample_input text,
    sample_output text,
    availability character varying(20) DEFAULT 'private'::character varying NOT NULL,
    price_eur numeric(8,2) DEFAULT 0,
    price_type character varying(20) DEFAULT 'one_time'::character varying,
    downloads_count integer DEFAULT 0,
    rating_avg numeric(3,2) DEFAULT 0,
    rating_count integer DEFAULT 0,
    status character varying(20) DEFAULT 'draft'::character varying,
    version character varying(20) DEFAULT '1.0.0'::character varying,
    tags jsonb DEFAULT '[]'::jsonb,
    icon character varying(10) DEFAULT '📄'::character varying,
    featured boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.mapping_templates OWNER TO buddyliko_user;

--
-- Name: org_budgets; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.org_budgets (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    budget_eur numeric(10,2) DEFAULT 0 NOT NULL,
    alert_pct jsonb DEFAULT '[50, 75, 90, 100]'::jsonb,
    auto_block boolean DEFAULT false,
    block_message text DEFAULT 'Budget mensile superato. Contatta l''amministratore.'::text,
    notified_pcts jsonb DEFAULT '[]'::jsonb,
    current_month character varying(7),
    is_blocked boolean DEFAULT false,
    blocked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.org_budgets OWNER TO buddyliko_user;

--
-- Name: org_members; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.org_members (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    user_id integer NOT NULL,
    role character varying(30) DEFAULT 'operator'::character varying NOT NULL,
    status character varying(20) DEFAULT 'active'::character varying,
    invited_by integer,
    invited_at timestamp with time zone,
    joined_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.org_members OWNER TO buddyliko_user;

--
-- Name: organizations; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.organizations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name character varying(255) NOT NULL,
    slug character varying(100) NOT NULL,
    org_type character varying(30) DEFAULT 'company'::character varying NOT NULL,
    parent_org_id uuid,
    depth integer DEFAULT 0 NOT NULL,
    hierarchy_path text,
    owner_user_id integer,
    plan character varying(50) DEFAULT 'FREE'::character varying NOT NULL,
    plan_started_at timestamp with time zone,
    plan_expires_at timestamp with time zone,
    stripe_customer_id character varying(255),
    stripe_subscription_id character varying(255),
    billing_email character varying(255),
    max_users integer,
    max_groups integer,
    max_api_tokens integer,
    max_transforms_month integer,
    max_ai_calls_month integer,
    max_storage_bytes bigint,
    max_partners integer,
    partnership_model character varying(30),
    revenue_share_pct numeric(5,2),
    custom_pricing jsonb DEFAULT '{}'::jsonb,
    vat_number character varying(50),
    fiscal_code character varying(50),
    sdi_code character varying(10),
    pec_email character varying(255),
    country character varying(3),
    currency character varying(3) DEFAULT 'EUR'::character varying,
    industry character varying(100),
    website character varying(500),
    logo_url character varying(500),
    settings jsonb DEFAULT '{}'::jsonb,
    status character varying(20) DEFAULT 'active'::character varying NOT NULL,
    trial_ends_at timestamp with time zone,
    suspended_at timestamp with time zone,
    suspended_reason text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.organizations OWNER TO buddyliko_user;

--
-- Name: plan_pricing; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.plan_pricing (
    id integer NOT NULL,
    plan character varying(50) NOT NULL,
    monthly_fee_eur numeric(10,2) DEFAULT 0 NOT NULL,
    yearly_fee_eur numeric(10,2) DEFAULT 0 NOT NULL,
    included_transforms integer DEFAULT 0 NOT NULL,
    included_ai_calls integer DEFAULT 0 NOT NULL,
    included_storage_mb integer DEFAULT 0 NOT NULL,
    included_users integer DEFAULT 1 NOT NULL,
    included_api_tokens integer DEFAULT 0 NOT NULL,
    included_partners integer DEFAULT 0 NOT NULL,
    included_groups integer DEFAULT 1 NOT NULL,
    per_transform_eur numeric(8,4) DEFAULT 0 NOT NULL,
    per_ai_call_eur numeric(8,4) DEFAULT 0 NOT NULL,
    per_gb_storage_eur numeric(8,4) DEFAULT 0 NOT NULL,
    per_extra_user_eur numeric(8,4) DEFAULT 0 NOT NULL,
    ai_markup_pct numeric(5,2) DEFAULT 30.00 NOT NULL,
    max_transforms_month integer DEFAULT 0 NOT NULL,
    max_ai_calls_month integer DEFAULT 0 NOT NULL,
    max_storage_mb integer DEFAULT 0 NOT NULL,
    max_users integer DEFAULT 0 NOT NULL,
    max_api_tokens integer DEFAULT 0 NOT NULL,
    max_partners integer DEFAULT 0 NOT NULL,
    max_groups integer DEFAULT 0 NOT NULL,
    max_file_size_mb integer DEFAULT 50 NOT NULL,
    max_sub_orgs integer DEFAULT 0 NOT NULL,
    max_depth integer DEFAULT 0 NOT NULL,
    features jsonb DEFAULT '{}'::jsonb NOT NULL,
    effective_from date DEFAULT CURRENT_DATE NOT NULL,
    effective_to date,
    active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.plan_pricing OWNER TO buddyliko_user;

--
-- Name: plan_pricing_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.plan_pricing_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.plan_pricing_id_seq OWNER TO buddyliko_user;

--
-- Name: plan_pricing_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.plan_pricing_id_seq OWNED BY public.plan_pricing.id;


--
-- Name: pricing_rules; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.pricing_rules (
    id character varying(36) NOT NULL,
    user_id character varying(255),
    group_id character varying(255),
    rule_type character varying(50) NOT NULL,
    plan character varying(50) DEFAULT 'CUSTOM'::character varying,
    custom_price_eur numeric(10,2),
    note text,
    created_by character varying(255),
    active boolean DEFAULT true,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.pricing_rules OWNER TO buddyliko_user;

--
-- Name: projects; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.projects (
    id integer NOT NULL,
    user_id character varying(255),
    name character varying(255) NOT NULL,
    project_data jsonb,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    org_id uuid
);


ALTER TABLE public.projects OWNER TO buddyliko_user;

--
-- Name: projects_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.projects_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.projects_id_seq OWNER TO buddyliko_user;

--
-- Name: projects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.projects_id_seq OWNED BY public.projects.id;


--
-- Name: schedule_runs; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.schedule_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    schedule_id uuid NOT NULL,
    org_id uuid NOT NULL,
    status character varying(20) DEFAULT 'running'::character varying,
    started_at timestamp with time zone DEFAULT now(),
    completed_at timestamp with time zone,
    duration_ms integer,
    result jsonb,
    error text
);


ALTER TABLE public.schedule_runs OWNER TO buddyliko_user;

--
-- Name: schedules; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.schedules (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    user_id integer,
    name character varying(255) NOT NULL,
    description text,
    schedule_type character varying(30) DEFAULT 'transform'::character varying NOT NULL,
    cron_expr character varying(100) NOT NULL,
    timezone character varying(50) DEFAULT 'Europe/Rome'::character varying,
    config jsonb DEFAULT '{}'::jsonb,
    status character varying(20) DEFAULT 'active'::character varying,
    last_run_at timestamp with time zone,
    next_run_at timestamp with time zone,
    run_count integer DEFAULT 0,
    fail_count integer DEFAULT 0,
    max_runs integer DEFAULT 0,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.schedules OWNER TO buddyliko_user;

--
-- Name: schemas; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.schemas (
    id integer NOT NULL,
    user_id character varying(255),
    name character varying(255) NOT NULL,
    description text,
    format character varying(50),
    schema_data jsonb,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    org_id uuid
);


ALTER TABLE public.schemas OWNER TO buddyliko_user;

--
-- Name: schemas_id_seq1; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.schemas_id_seq1
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.schemas_id_seq1 OWNER TO buddyliko_user;

--
-- Name: schemas_id_seq1; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.schemas_id_seq1 OWNED BY public.schemas.id;


--
-- Name: share_links; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.share_links (
    id character varying(36) NOT NULL,
    file_id character varying(36),
    token character varying(255) NOT NULL,
    created_by character varying(255) NOT NULL,
    expires_at timestamp without time zone NOT NULL,
    max_uses integer,
    uses integer DEFAULT 0,
    note text,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.share_links OWNER TO buddyliko_user;

--
-- Name: subscriptions; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.subscriptions (
    id character varying(36) NOT NULL,
    user_id character varying(255) NOT NULL,
    stripe_customer_id character varying(255),
    stripe_subscription_id character varying(255),
    plan character varying(50) DEFAULT 'FREE'::character varying NOT NULL,
    status character varying(50) DEFAULT 'active'::character varying NOT NULL,
    period_start timestamp with time zone,
    period_end timestamp with time zone,
    cancel_at_period_end boolean DEFAULT false,
    override_by_admin character varying(255),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    metadata jsonb DEFAULT '{}'::jsonb,
    org_id uuid
);


ALTER TABLE public.subscriptions OWNER TO buddyliko_user;

--
-- Name: system_settings; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.system_settings (
    key character varying(100) NOT NULL,
    value text NOT NULL,
    updated_at timestamp with time zone DEFAULT now(),
    updated_by character varying(255)
);


ALTER TABLE public.system_settings OWNER TO buddyliko_user;

--
-- Name: template_purchases; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.template_purchases (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    template_id uuid NOT NULL,
    buyer_org_id uuid NOT NULL,
    buyer_user_id integer,
    price_paid_eur numeric(8,2) DEFAULT 0,
    price_type character varying(20),
    status character varying(20) DEFAULT 'active'::character varying,
    installed_at timestamp with time zone DEFAULT now(),
    expires_at timestamp with time zone
);


ALTER TABLE public.template_purchases OWNER TO buddyliko_user;

--
-- Name: template_reviews; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.template_reviews (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    template_id uuid NOT NULL,
    reviewer_org_id uuid,
    reviewer_user_id integer,
    rating integer NOT NULL,
    title character varying(200),
    body text,
    status character varying(20) DEFAULT 'published'::character varying,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT template_reviews_rating_check CHECK (((rating >= 1) AND (rating <= 5)))
);


ALTER TABLE public.template_reviews OWNER TO buddyliko_user;

--
-- Name: token_audit_log; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.token_audit_log (
    id bigint NOT NULL,
    token_id uuid NOT NULL,
    org_id uuid NOT NULL,
    event_type character varying(30) NOT NULL,
    ip_address character varying(45),
    user_agent character varying(500),
    endpoint character varying(200),
    http_status integer,
    actor_user_id integer,
    details jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.token_audit_log OWNER TO buddyliko_user;

--
-- Name: token_audit_log_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.token_audit_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.token_audit_log_id_seq OWNER TO buddyliko_user;

--
-- Name: token_audit_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.token_audit_log_id_seq OWNED BY public.token_audit_log.id;


--
-- Name: trading_partners; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.trading_partners (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    name character varying(255) NOT NULL,
    code character varying(50),
    partner_type character varying(50) DEFAULT 'other'::character varying NOT NULL,
    vat_number character varying(50),
    gln character varying(20),
    duns character varying(20),
    edi_id character varying(50),
    peppol_id character varying(100),
    sdi_code character varying(10),
    protocols jsonb DEFAULT '[]'::jsonb,
    preferred_formats jsonb DEFAULT '{}'::jsonb,
    default_mappings jsonb DEFAULT '{}'::jsonb,
    contact_name character varying(255),
    contact_email character varying(255),
    contact_phone character varying(50),
    status character varying(20) DEFAULT 'active'::character varying,
    notes text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.trading_partners OWNER TO buddyliko_user;

--
-- Name: transformation_costs; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.transformation_costs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    auth_type character varying(20) NOT NULL,
    auth_id character varying(255) NOT NULL,
    auth_name character varying(255),
    environment character varying(10) DEFAULT 'live'::character varying NOT NULL,
    partner_id uuid,
    tags jsonb DEFAULT '{}'::jsonb,
    job_id character varying(36),
    operation character varying(50) NOT NULL,
    input_format character varying(30),
    output_format character varying(30),
    input_bytes bigint DEFAULT 0,
    output_bytes bigint DEFAULT 0,
    records_count integer DEFAULT 0,
    ai_provider character varying(30),
    ai_model character varying(80),
    ai_input_tokens integer DEFAULT 0,
    ai_output_tokens integer DEFAULT 0,
    ai_cost_usd numeric(10,6) DEFAULT 0,
    platform_cost_eur numeric(10,4) DEFAULT 0,
    billable_amount_eur numeric(10,4) DEFAULT 0,
    margin_eur numeric(10,4) DEFAULT 0,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    duration_ms integer,
    status character varying(20) DEFAULT 'completed'::character varying,
    error_message text,
    billing_month character varying(7) NOT NULL,
    invoiced boolean DEFAULT false,
    invoice_id character varying(36),
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.transformation_costs OWNER TO buddyliko_user;

--
-- Name: usage_aggregates; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.usage_aggregates (
    id integer NOT NULL,
    org_id uuid NOT NULL,
    period_type character varying(10) NOT NULL,
    period_key character varying(10) NOT NULL,
    auth_type character varying(20) DEFAULT 'all'::character varying NOT NULL,
    environment character varying(10) DEFAULT 'live'::character varying NOT NULL,
    transforms_count integer DEFAULT 0,
    validations_count integer DEFAULT 0,
    ai_calls_count integer DEFAULT 0,
    ai_codegen_count integer DEFAULT 0,
    input_bytes_total bigint DEFAULT 0,
    output_bytes_total bigint DEFAULT 0,
    records_total bigint DEFAULT 0,
    ai_cost_usd_total numeric(12,4) DEFAULT 0,
    platform_cost_eur numeric(12,4) DEFAULT 0,
    billable_eur_total numeric(12,4) DEFAULT 0,
    margin_eur_total numeric(12,4) DEFAULT 0,
    avg_duration_ms integer DEFAULT 0,
    error_count integer DEFAULT 0,
    unique_users integer DEFAULT 0,
    unique_tokens integer DEFAULT 0,
    unique_partners integer DEFAULT 0,
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.usage_aggregates OWNER TO buddyliko_user;

--
-- Name: usage_aggregates_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.usage_aggregates_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.usage_aggregates_id_seq OWNER TO buddyliko_user;

--
-- Name: usage_aggregates_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.usage_aggregates_id_seq OWNED BY public.usage_aggregates.id;


--
-- Name: usage_counters; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.usage_counters (
    user_id character varying(255) NOT NULL,
    month character varying(7) NOT NULL,
    transforms_count integer DEFAULT 0,
    api_calls_count integer DEFAULT 0,
    bytes_processed bigint DEFAULT 0,
    codegen_count integer DEFAULT 0,
    org_id uuid
);


ALTER TABLE public.usage_counters OWNER TO buddyliko_user;

--
-- Name: user_auth_providers; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.user_auth_providers (
    id integer NOT NULL,
    user_id character varying(255) NOT NULL,
    provider character varying(50) NOT NULL,
    provider_id character varying(255) NOT NULL,
    provider_email character varying(255),
    provider_name character varying(255),
    linked_at timestamp with time zone DEFAULT now(),
    last_used_at timestamp with time zone
);


ALTER TABLE public.user_auth_providers OWNER TO buddyliko_user;

--
-- Name: user_auth_providers_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.user_auth_providers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.user_auth_providers_id_seq OWNER TO buddyliko_user;

--
-- Name: user_auth_providers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.user_auth_providers_id_seq OWNED BY public.user_auth_providers.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.users (
    id integer NOT NULL,
    email character varying(255) NOT NULL,
    password_hash character varying(255),
    name character varying(255),
    role character varying(50) DEFAULT 'USER'::character varying,
    status character varying(50) DEFAULT 'APPROVED'::character varying,
    plan character varying(50) DEFAULT 'FREE'::character varying,
    auth_provider character varying(50),
    auth_provider_id character varying(255),
    created_at timestamp without time zone,
    email_verified boolean DEFAULT false,
    mfa_enabled boolean DEFAULT false,
    mfa_method character varying(50),
    mfa_secret character varying(255),
    mfa_totp_pending character varying(255),
    group_id integer,
    default_org_id uuid
);


ALTER TABLE public.users OWNER TO buddyliko_user;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: buddyliko_user
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.users_id_seq OWNER TO buddyliko_user;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: buddyliko_user
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: v_ai_monthly_spend; Type: VIEW; Schema: public; Owner: buddyliko_user
--

CREATE VIEW public.v_ai_monthly_spend AS
 SELECT month,
    provider,
    model,
    count(*) AS call_count,
    sum(input_tokens) AS total_input_tokens,
    sum(output_tokens) AS total_output_tokens,
    sum(total_tokens) AS total_tokens,
    sum(cost_usd) AS total_cost_usd,
    avg(duration_ms) AS avg_duration_ms
   FROM public.ai_token_usage
  GROUP BY month, provider, model
  ORDER BY month DESC, provider;


ALTER VIEW public.v_ai_monthly_spend OWNER TO buddyliko_user;

--
-- Name: webhook_deliveries; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.webhook_deliveries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    webhook_id uuid NOT NULL,
    org_id uuid NOT NULL,
    event character varying(60) NOT NULL,
    payload jsonb,
    status_code integer,
    response_body text,
    error text,
    attempt integer DEFAULT 1,
    success boolean DEFAULT false,
    duration_ms integer,
    delivered_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.webhook_deliveries OWNER TO buddyliko_user;

--
-- Name: webhooks; Type: TABLE; Schema: public; Owner: buddyliko_user
--

CREATE TABLE public.webhooks (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid NOT NULL,
    name character varying(255) NOT NULL,
    url text NOT NULL,
    secret character varying(255),
    events jsonb DEFAULT '[]'::jsonb NOT NULL,
    headers jsonb DEFAULT '{}'::jsonb,
    is_active boolean DEFAULT true,
    created_by integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.webhooks OWNER TO buddyliko_user;

--
-- Name: ai_budget_snapshots id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.ai_budget_snapshots ALTER COLUMN id SET DEFAULT nextval('public.ai_budget_snapshots_id_seq'::regclass);


--
-- Name: ai_provider_balance id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.ai_provider_balance ALTER COLUMN id SET DEFAULT nextval('public.ai_provider_balance_id_seq'::regclass);


--
-- Name: ai_token_usage id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.ai_token_usage ALTER COLUMN id SET DEFAULT nextval('public.ai_token_usage_id_seq'::regclass);


--
-- Name: alert_events id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.alert_events ALTER COLUMN id SET DEFAULT nextval('public.alert_events_id_seq'::regclass);


--
-- Name: in_app_notifications id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.in_app_notifications ALTER COLUMN id SET DEFAULT nextval('public.in_app_notifications_id_seq'::regclass);


--
-- Name: plan_pricing id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.plan_pricing ALTER COLUMN id SET DEFAULT nextval('public.plan_pricing_id_seq'::regclass);


--
-- Name: projects id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.projects ALTER COLUMN id SET DEFAULT nextval('public.projects_id_seq'::regclass);


--
-- Name: schemas id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schemas ALTER COLUMN id SET DEFAULT nextval('public.schemas_id_seq1'::regclass);


--
-- Name: token_audit_log id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.token_audit_log ALTER COLUMN id SET DEFAULT nextval('public.token_audit_log_id_seq'::regclass);


--
-- Name: usage_aggregates id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.usage_aggregates ALTER COLUMN id SET DEFAULT nextval('public.usage_aggregates_id_seq'::regclass);


--
-- Name: user_auth_providers id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.user_auth_providers ALTER COLUMN id SET DEFAULT nextval('public.user_auth_providers_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: ai_budget_snapshots ai_budget_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.ai_budget_snapshots
    ADD CONSTRAINT ai_budget_snapshots_pkey PRIMARY KEY (id);


--
-- Name: ai_budget_snapshots ai_budget_snapshots_provider_snapshot_date_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.ai_budget_snapshots
    ADD CONSTRAINT ai_budget_snapshots_provider_snapshot_date_key UNIQUE (provider, snapshot_date);


--
-- Name: ai_provider_balance ai_provider_balance_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.ai_provider_balance
    ADD CONSTRAINT ai_provider_balance_pkey PRIMARY KEY (id);


--
-- Name: ai_token_usage ai_token_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.ai_token_usage
    ADD CONSTRAINT ai_token_usage_pkey PRIMARY KEY (id);


--
-- Name: alert_events alert_events_period_key_channel_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.alert_events
    ADD CONSTRAINT alert_events_period_key_channel_key UNIQUE (period_key, channel);


--
-- Name: alert_events alert_events_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.alert_events
    ADD CONSTRAINT alert_events_pkey PRIMARY KEY (id);


--
-- Name: alert_rules alert_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.alert_rules
    ADD CONSTRAINT alert_rules_pkey PRIMARY KEY (id);


--
-- Name: alert_rules alert_rules_rule_type_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.alert_rules
    ADD CONSTRAINT alert_rules_rule_type_key UNIQUE (rule_type);


--
-- Name: api_tokens api_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT api_tokens_pkey PRIMARY KEY (id);


--
-- Name: audit_logs audit_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.audit_logs
    ADD CONSTRAINT audit_logs_pkey PRIMARY KEY (id);


--
-- Name: batch_items batch_items_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.batch_items
    ADD CONSTRAINT batch_items_pkey PRIMARY KEY (id);


--
-- Name: batch_jobs batch_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.batch_jobs
    ADD CONSTRAINT batch_jobs_pkey PRIMARY KEY (id);


--
-- Name: billing_events billing_events_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.billing_events
    ADD CONSTRAINT billing_events_pkey PRIMARY KEY (id);


--
-- Name: billing_events billing_events_stripe_event_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.billing_events
    ADD CONSTRAINT billing_events_stripe_event_id_key UNIQUE (stripe_event_id);


--
-- Name: budget_alerts budget_alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.budget_alerts
    ADD CONSTRAINT budget_alerts_pkey PRIMARY KEY (id);


--
-- Name: db_connections db_connections_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.db_connections
    ADD CONSTRAINT db_connections_pkey PRIMARY KEY (id);


--
-- Name: discount_codes discount_codes_code_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.discount_codes
    ADD CONSTRAINT discount_codes_code_key UNIQUE (code);


--
-- Name: discount_codes discount_codes_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.discount_codes
    ADD CONSTRAINT discount_codes_pkey PRIMARY KEY (id);


--
-- Name: file_permissions file_permissions_file_id_user_id_group_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.file_permissions
    ADD CONSTRAINT file_permissions_file_id_user_id_group_id_key UNIQUE NULLS NOT DISTINCT (file_id, user_id, group_id);


--
-- Name: file_permissions file_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.file_permissions
    ADD CONSTRAINT file_permissions_pkey PRIMARY KEY (id);


--
-- Name: files files_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_pkey PRIMARY KEY (id);


--
-- Name: group_invitations group_invitations_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.group_invitations
    ADD CONSTRAINT group_invitations_pkey PRIMARY KEY (id);


--
-- Name: group_invitations group_invitations_token_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.group_invitations
    ADD CONSTRAINT group_invitations_token_key UNIQUE (token);


--
-- Name: group_members group_members_group_id_user_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.group_members
    ADD CONSTRAINT group_members_group_id_user_id_key UNIQUE (group_id, user_id);


--
-- Name: group_members group_members_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.group_members
    ADD CONSTRAINT group_members_pkey PRIMARY KEY (id);


--
-- Name: groups groups_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.groups
    ADD CONSTRAINT groups_pkey PRIMARY KEY (id);


--
-- Name: in_app_notifications in_app_notifications_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.in_app_notifications
    ADD CONSTRAINT in_app_notifications_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: mapping_templates mapping_templates_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.mapping_templates
    ADD CONSTRAINT mapping_templates_pkey PRIMARY KEY (id);


--
-- Name: mapping_templates mapping_templates_slug_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.mapping_templates
    ADD CONSTRAINT mapping_templates_slug_key UNIQUE (slug);


--
-- Name: org_budgets org_budgets_org_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_budgets
    ADD CONSTRAINT org_budgets_org_id_key UNIQUE (org_id);


--
-- Name: org_budgets org_budgets_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_budgets
    ADD CONSTRAINT org_budgets_pkey PRIMARY KEY (id);


--
-- Name: org_members org_members_org_id_user_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_org_id_user_id_key UNIQUE (org_id, user_id);


--
-- Name: org_members org_members_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_pkey PRIMARY KEY (id);


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);


--
-- Name: organizations organizations_slug_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_slug_key UNIQUE (slug);


--
-- Name: plan_pricing plan_pricing_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.plan_pricing
    ADD CONSTRAINT plan_pricing_pkey PRIMARY KEY (id);


--
-- Name: pricing_rules pricing_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.pricing_rules
    ADD CONSTRAINT pricing_rules_pkey PRIMARY KEY (id);


--
-- Name: projects projects_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (id);


--
-- Name: schedule_runs schedule_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schedule_runs
    ADD CONSTRAINT schedule_runs_pkey PRIMARY KEY (id);


--
-- Name: schedules schedules_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schedules
    ADD CONSTRAINT schedules_pkey PRIMARY KEY (id);


--
-- Name: schemas schemas_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schemas
    ADD CONSTRAINT schemas_pkey PRIMARY KEY (id);


--
-- Name: share_links share_links_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.share_links
    ADD CONSTRAINT share_links_pkey PRIMARY KEY (id);


--
-- Name: share_links share_links_token_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.share_links
    ADD CONSTRAINT share_links_token_key UNIQUE (token);


--
-- Name: subscriptions subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.subscriptions
    ADD CONSTRAINT subscriptions_pkey PRIMARY KEY (id);


--
-- Name: system_settings system_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT system_settings_pkey PRIMARY KEY (key);


--
-- Name: template_purchases template_purchases_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_purchases
    ADD CONSTRAINT template_purchases_pkey PRIMARY KEY (id);


--
-- Name: template_purchases template_purchases_template_id_buyer_org_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_purchases
    ADD CONSTRAINT template_purchases_template_id_buyer_org_id_key UNIQUE (template_id, buyer_org_id);


--
-- Name: template_reviews template_reviews_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_reviews
    ADD CONSTRAINT template_reviews_pkey PRIMARY KEY (id);


--
-- Name: template_reviews template_reviews_template_id_reviewer_org_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_reviews
    ADD CONSTRAINT template_reviews_template_id_reviewer_org_id_key UNIQUE (template_id, reviewer_org_id);


--
-- Name: token_audit_log token_audit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.token_audit_log
    ADD CONSTRAINT token_audit_log_pkey PRIMARY KEY (id);


--
-- Name: trading_partners trading_partners_org_id_code_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.trading_partners
    ADD CONSTRAINT trading_partners_org_id_code_key UNIQUE (org_id, code);


--
-- Name: trading_partners trading_partners_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.trading_partners
    ADD CONSTRAINT trading_partners_pkey PRIMARY KEY (id);


--
-- Name: transformation_costs transformation_costs_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.transformation_costs
    ADD CONSTRAINT transformation_costs_pkey PRIMARY KEY (id);


--
-- Name: usage_aggregates usage_aggregates_org_id_period_type_period_key_auth_type_en_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.usage_aggregates
    ADD CONSTRAINT usage_aggregates_org_id_period_type_period_key_auth_type_en_key UNIQUE (org_id, period_type, period_key, auth_type, environment);


--
-- Name: usage_aggregates usage_aggregates_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.usage_aggregates
    ADD CONSTRAINT usage_aggregates_pkey PRIMARY KEY (id);


--
-- Name: usage_counters usage_counters_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.usage_counters
    ADD CONSTRAINT usage_counters_pkey PRIMARY KEY (user_id, month);


--
-- Name: user_auth_providers user_auth_providers_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.user_auth_providers
    ADD CONSTRAINT user_auth_providers_pkey PRIMARY KEY (id);


--
-- Name: user_auth_providers user_auth_providers_provider_provider_id_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.user_auth_providers
    ADD CONSTRAINT user_auth_providers_provider_provider_id_key UNIQUE (provider, provider_id);


--
-- Name: user_auth_providers user_auth_providers_user_id_provider_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.user_auth_providers
    ADD CONSTRAINT user_auth_providers_user_id_provider_key UNIQUE (user_id, provider);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: webhook_deliveries webhook_deliveries_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_pkey PRIMARY KEY (id);


--
-- Name: webhooks webhooks_pkey; Type: CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.webhooks
    ADD CONSTRAINT webhooks_pkey PRIMARY KEY (id);


--
-- Name: idx_ai_balance_provider; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ai_balance_provider ON public.ai_provider_balance USING btree (provider, checked_at DESC);


--
-- Name: idx_ai_token_usage_month; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ai_token_usage_month ON public.ai_token_usage USING btree (month);


--
-- Name: idx_ai_token_usage_provider; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ai_token_usage_provider ON public.ai_token_usage USING btree (provider, created_at);


--
-- Name: idx_ai_token_usage_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ai_token_usage_user ON public.ai_token_usage USING btree (user_id, month);


--
-- Name: idx_ai_usage_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ai_usage_org ON public.ai_token_usage USING btree (org_id);


--
-- Name: idx_aibs_prov; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_aibs_prov ON public.ai_budget_snapshots USING btree (provider, snapshot_date DESC);


--
-- Name: idx_at_env; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_at_env ON public.api_tokens USING btree (org_id, environment);


--
-- Name: idx_at_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_at_org ON public.api_tokens USING btree (org_id, status);


--
-- Name: idx_at_partner; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_at_partner ON public.api_tokens USING btree (partner_id) WHERE (partner_id IS NOT NULL);


--
-- Name: idx_at_prefix; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_at_prefix ON public.api_tokens USING btree (token_prefix);


--
-- Name: idx_at_tags; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_at_tags ON public.api_tokens USING gin (tags);


--
-- Name: idx_audit_action; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_audit_action ON public.audit_logs USING btree (action);


--
-- Name: idx_audit_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_audit_org ON public.audit_logs USING btree (org_id);


--
-- Name: idx_audit_outcome; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_audit_outcome ON public.audit_logs USING btree (outcome);


--
-- Name: idx_audit_timestamp; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_audit_timestamp ON public.audit_logs USING btree ("timestamp" DESC);


--
-- Name: idx_audit_user_id; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_audit_user_id ON public.audit_logs USING btree (user_id);


--
-- Name: idx_ba_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ba_org ON public.budget_alerts USING btree (org_id, month);


--
-- Name: idx_bi_batch; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_bi_batch ON public.batch_items USING btree (batch_id, seq);


--
-- Name: idx_bj_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_bj_org ON public.batch_jobs USING btree (org_id, status);


--
-- Name: idx_bj_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_bj_user ON public.batch_jobs USING btree (user_id);


--
-- Name: idx_dbconn_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_dbconn_org ON public.db_connections USING btree (org_id);


--
-- Name: idx_dbconn_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_dbconn_user ON public.db_connections USING btree (user_id);


--
-- Name: idx_dc_code; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_dc_code ON public.discount_codes USING btree (code, active);


--
-- Name: idx_files_group; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_files_group ON public.files USING btree (group_id);


--
-- Name: idx_files_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_files_org ON public.files USING btree (org_id);


--
-- Name: idx_files_owner; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_files_owner ON public.files USING btree (owner_id);


--
-- Name: idx_files_type; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_files_type ON public.files USING btree (file_type);


--
-- Name: idx_group_members_group; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_group_members_group ON public.group_members USING btree (group_id);


--
-- Name: idx_group_members_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_group_members_user ON public.group_members USING btree (user_id);


--
-- Name: idx_groups_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_groups_org ON public.groups USING btree (org_id);


--
-- Name: idx_groups_type; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_groups_type ON public.groups USING btree (group_type);


--
-- Name: idx_ian_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ian_user ON public.in_app_notifications USING btree (user_id, read_at);


--
-- Name: idx_invitations_email; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_invitations_email ON public.group_invitations USING btree (email);


--
-- Name: idx_invitations_group; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_invitations_group ON public.group_invitations USING btree (group_id);


--
-- Name: idx_invitations_token; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_invitations_token ON public.group_invitations USING btree (token);


--
-- Name: idx_jobs_created; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_jobs_created ON public.jobs USING btree (created_at DESC);


--
-- Name: idx_jobs_env; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_jobs_env ON public.jobs USING btree (environment);


--
-- Name: idx_jobs_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_jobs_org ON public.jobs USING btree (org_id);


--
-- Name: idx_jobs_status; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_jobs_status ON public.jobs USING btree (status);


--
-- Name: idx_jobs_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_jobs_user ON public.jobs USING btree (user_id);


--
-- Name: idx_mt_author; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_mt_author ON public.mapping_templates USING btree (author_org_id);


--
-- Name: idx_mt_availability; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_mt_availability ON public.mapping_templates USING btree (availability, status);


--
-- Name: idx_mt_category; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_mt_category ON public.mapping_templates USING btree (category, status);


--
-- Name: idx_mt_formats; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_mt_formats ON public.mapping_templates USING btree (input_format, output_format);


--
-- Name: idx_mt_slug; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_mt_slug ON public.mapping_templates USING btree (slug);


--
-- Name: idx_om_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_om_org ON public.org_members USING btree (org_id);


--
-- Name: idx_om_role; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_om_role ON public.org_members USING btree (org_id, role);


--
-- Name: idx_om_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_om_user ON public.org_members USING btree (user_id);


--
-- Name: idx_org_hierarchy; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_org_hierarchy ON public.organizations USING btree (hierarchy_path text_pattern_ops);


--
-- Name: idx_org_owner; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_org_owner ON public.organizations USING btree (owner_user_id);


--
-- Name: idx_org_parent; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_org_parent ON public.organizations USING btree (parent_org_id);


--
-- Name: idx_org_slug; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_org_slug ON public.organizations USING btree (slug);


--
-- Name: idx_org_status; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_org_status ON public.organizations USING btree (status);


--
-- Name: idx_org_type; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_org_type ON public.organizations USING btree (org_type, status);


--
-- Name: idx_perms_file; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_perms_file ON public.file_permissions USING btree (file_id);


--
-- Name: idx_pr_group; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_pr_group ON public.pricing_rules USING btree (group_id, active);


--
-- Name: idx_pr_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_pr_user ON public.pricing_rules USING btree (user_id, active);


--
-- Name: idx_projects_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_projects_org ON public.projects USING btree (org_id);


--
-- Name: idx_sch_next; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_sch_next ON public.schedules USING btree (next_run_at, status);


--
-- Name: idx_sch_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_sch_org ON public.schedules USING btree (org_id, status);


--
-- Name: idx_schemas_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_schemas_org ON public.schemas USING btree (org_id);


--
-- Name: idx_share_token; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_share_token ON public.share_links USING btree (token);


--
-- Name: idx_sr_sched; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_sr_sched ON public.schedule_runs USING btree (schedule_id, started_at DESC);


--
-- Name: idx_sub_stripe; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_sub_stripe ON public.subscriptions USING btree (stripe_subscription_id);


--
-- Name: idx_sub_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_sub_user ON public.subscriptions USING btree (user_id);


--
-- Name: idx_subs_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_subs_org ON public.subscriptions USING btree (org_id);


--
-- Name: idx_tal_event; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tal_event ON public.token_audit_log USING btree (event_type, created_at DESC);


--
-- Name: idx_tal_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tal_org ON public.token_audit_log USING btree (org_id, created_at DESC);


--
-- Name: idx_tal_token; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tal_token ON public.token_audit_log USING btree (token_id, created_at DESC);


--
-- Name: idx_tc_auth; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_auth ON public.transformation_costs USING btree (auth_type, auth_id);


--
-- Name: idx_tc_env; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_env ON public.transformation_costs USING btree (environment);


--
-- Name: idx_tc_invoiced; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_invoiced ON public.transformation_costs USING btree (invoiced, billing_month) WHERE (NOT invoiced);


--
-- Name: idx_tc_operation; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_operation ON public.transformation_costs USING btree (operation, billing_month);


--
-- Name: idx_tc_org_env_month; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_org_env_month ON public.transformation_costs USING btree (org_id, environment, billing_month);


--
-- Name: idx_tc_org_month; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_org_month ON public.transformation_costs USING btree (org_id, billing_month);


--
-- Name: idx_tc_partner; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_partner ON public.transformation_costs USING btree (partner_id) WHERE (partner_id IS NOT NULL);


--
-- Name: idx_tc_tags; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tc_tags ON public.transformation_costs USING gin (tags);


--
-- Name: idx_tp_buyer; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tp_buyer ON public.template_purchases USING btree (buyer_org_id);


--
-- Name: idx_tp_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tp_org ON public.trading_partners USING btree (org_id);


--
-- Name: idx_tp_peppol; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tp_peppol ON public.trading_partners USING btree (peppol_id) WHERE (peppol_id IS NOT NULL);


--
-- Name: idx_tp_type; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tp_type ON public.trading_partners USING btree (org_id, partner_type);


--
-- Name: idx_tp_vat; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tp_vat ON public.trading_partners USING btree (vat_number) WHERE (vat_number IS NOT NULL);


--
-- Name: idx_tr_template; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_tr_template ON public.template_reviews USING btree (template_id);


--
-- Name: idx_ua_lookup; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_ua_lookup ON public.usage_aggregates USING btree (org_id, period_type, period_key);


--
-- Name: idx_uap_provider; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_uap_provider ON public.user_auth_providers USING btree (provider, provider_id);


--
-- Name: idx_uap_user; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_uap_user ON public.user_auth_providers USING btree (user_id);


--
-- Name: idx_wd_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_wd_org ON public.webhook_deliveries USING btree (org_id, delivered_at DESC);


--
-- Name: idx_wd_webhook; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_wd_webhook ON public.webhook_deliveries USING btree (webhook_id);


--
-- Name: idx_wh_org; Type: INDEX; Schema: public; Owner: buddyliko_user
--

CREATE INDEX idx_wh_org ON public.webhooks USING btree (org_id, is_active);


--
-- Name: api_tokens api_tokens_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT api_tokens_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: api_tokens api_tokens_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT api_tokens_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: api_tokens api_tokens_revoked_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT api_tokens_revoked_by_fkey FOREIGN KEY (revoked_by) REFERENCES public.users(id);


--
-- Name: batch_items batch_items_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.batch_items
    ADD CONSTRAINT batch_items_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.batch_jobs(id) ON DELETE CASCADE;


--
-- Name: batch_jobs batch_jobs_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.batch_jobs
    ADD CONSTRAINT batch_jobs_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: batch_jobs batch_jobs_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.batch_jobs
    ADD CONSTRAINT batch_jobs_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: budget_alerts budget_alerts_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.budget_alerts
    ADD CONSTRAINT budget_alerts_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: file_permissions file_permissions_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.file_permissions
    ADD CONSTRAINT file_permissions_file_id_fkey FOREIGN KEY (file_id) REFERENCES public.files(id) ON DELETE CASCADE;


--
-- Name: file_permissions file_permissions_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.file_permissions
    ADD CONSTRAINT file_permissions_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.groups(id) ON DELETE CASCADE;


--
-- Name: files files_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.groups(id) ON DELETE SET NULL;


--
-- Name: files files_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: group_invitations group_invitations_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.group_invitations
    ADD CONSTRAINT group_invitations_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.groups(id) ON DELETE CASCADE;


--
-- Name: group_members group_members_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.group_members
    ADD CONSTRAINT group_members_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.groups(id) ON DELETE CASCADE;


--
-- Name: groups groups_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.groups
    ADD CONSTRAINT groups_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: groups groups_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.groups
    ADD CONSTRAINT groups_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.groups(id) ON DELETE CASCADE;


--
-- Name: mapping_templates mapping_templates_author_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.mapping_templates
    ADD CONSTRAINT mapping_templates_author_org_id_fkey FOREIGN KEY (author_org_id) REFERENCES public.organizations(id);


--
-- Name: mapping_templates mapping_templates_author_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.mapping_templates
    ADD CONSTRAINT mapping_templates_author_user_id_fkey FOREIGN KEY (author_user_id) REFERENCES public.users(id);


--
-- Name: org_budgets org_budgets_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_budgets
    ADD CONSTRAINT org_budgets_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: org_members org_members_invited_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_invited_by_fkey FOREIGN KEY (invited_by) REFERENCES public.users(id);


--
-- Name: org_members org_members_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: org_members org_members_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.org_members
    ADD CONSTRAINT org_members_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: organizations organizations_owner_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id);


--
-- Name: organizations organizations_parent_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_parent_org_id_fkey FOREIGN KEY (parent_org_id) REFERENCES public.organizations(id) ON DELETE SET NULL;


--
-- Name: projects projects_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: schedule_runs schedule_runs_schedule_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schedule_runs
    ADD CONSTRAINT schedule_runs_schedule_id_fkey FOREIGN KEY (schedule_id) REFERENCES public.schedules(id) ON DELETE CASCADE;


--
-- Name: schedules schedules_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schedules
    ADD CONSTRAINT schedules_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: schedules schedules_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schedules
    ADD CONSTRAINT schedules_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: schemas schemas_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.schemas
    ADD CONSTRAINT schemas_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: share_links share_links_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.share_links
    ADD CONSTRAINT share_links_file_id_fkey FOREIGN KEY (file_id) REFERENCES public.files(id) ON DELETE CASCADE;


--
-- Name: template_purchases template_purchases_buyer_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_purchases
    ADD CONSTRAINT template_purchases_buyer_org_id_fkey FOREIGN KEY (buyer_org_id) REFERENCES public.organizations(id);


--
-- Name: template_purchases template_purchases_buyer_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_purchases
    ADD CONSTRAINT template_purchases_buyer_user_id_fkey FOREIGN KEY (buyer_user_id) REFERENCES public.users(id);


--
-- Name: template_purchases template_purchases_template_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_purchases
    ADD CONSTRAINT template_purchases_template_id_fkey FOREIGN KEY (template_id) REFERENCES public.mapping_templates(id);


--
-- Name: template_reviews template_reviews_reviewer_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_reviews
    ADD CONSTRAINT template_reviews_reviewer_org_id_fkey FOREIGN KEY (reviewer_org_id) REFERENCES public.organizations(id);


--
-- Name: template_reviews template_reviews_reviewer_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_reviews
    ADD CONSTRAINT template_reviews_reviewer_user_id_fkey FOREIGN KEY (reviewer_user_id) REFERENCES public.users(id);


--
-- Name: template_reviews template_reviews_template_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.template_reviews
    ADD CONSTRAINT template_reviews_template_id_fkey FOREIGN KEY (template_id) REFERENCES public.mapping_templates(id) ON DELETE CASCADE;


--
-- Name: trading_partners trading_partners_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.trading_partners
    ADD CONSTRAINT trading_partners_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: transformation_costs transformation_costs_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.transformation_costs
    ADD CONSTRAINT transformation_costs_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: usage_aggregates usage_aggregates_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.usage_aggregates
    ADD CONSTRAINT usage_aggregates_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: users users_default_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_default_org_id_fkey FOREIGN KEY (default_org_id) REFERENCES public.organizations(id);


--
-- Name: webhook_deliveries webhook_deliveries_webhook_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_webhook_id_fkey FOREIGN KEY (webhook_id) REFERENCES public.webhooks(id) ON DELETE CASCADE;


--
-- Name: webhooks webhooks_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.webhooks
    ADD CONSTRAINT webhooks_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: webhooks webhooks_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: buddyliko_user
--

ALTER TABLE ONLY public.webhooks
    ADD CONSTRAINT webhooks_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT ALL ON SCHEMA public TO buddyliko_user;


--
-- PostgreSQL database dump complete
--

\unrestrict kQWX82pRXAewyutWJv2vQEXcnZpzhEMVd17fE1JzHxCI0nsejCPyr1xan4zE0Bv

