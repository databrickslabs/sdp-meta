import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';

// feature cards removed — replaced by WhatIsSection below

const quickstart = `# Install
pip install 'databricks-labs-sdp-meta[mcp]'
databricks labs install sdp-meta

# Scaffold a bundle (zero prompts, dev-friendly defaults)
databricks labs sdp-meta bundle-init --quickstart

# Validate and deploy
databricks labs sdp-meta bundle-validate
databricks bundle deploy`;

function HeroBanner() {
  return (
    <header
      style={{
        background: 'linear-gradient(135deg, #1B3139 0%, #2d5060 100%)',
        padding: '4rem 0 3rem',
        textAlign: 'center',
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      <div className="container">
        <div style={{ marginBottom: '1rem' }}>
          <span
            style={{
              background: '#FF3621',
              color: '#fff',
              padding: '0.25rem 0.75rem',
              borderRadius: '99px',
              fontSize: '0.8rem',
              fontWeight: 700,
              letterSpacing: '0.05em',
              textTransform: 'uppercase',
            }}
          >
            Databricks Labs
          </span>
        </div>
        <h1
          style={{
            fontSize: 'clamp(2rem, 5vw, 3.5rem)',
            fontWeight: 800,
            color: '#ffffff',
            marginBottom: '1rem',
          }}
        >
          SDP-META
        </h1>
        <p
          style={{
            fontSize: 'clamp(1rem, 2vw, 1.3rem)',
            color: 'rgba(255,255,255,0.85)',
            maxWidth: '640px',
            margin: '0 auto 2rem',
            lineHeight: 1.6,
          }}
        >
          Metadata-driven framework for automated{' '}
          <strong style={{ color: '#ffffff' }}>Bronze and Silver</strong> pipelines on{' '}
          <strong style={{ color: '#ffffff' }}>Lakeflow Spark Declarative Pipelines</strong>.
          Define your pipeline in JSON or YAML — SDP-META handles the rest.
        </p>
        <div style={{ display: 'flex', gap: '1rem', justifyContent: 'center', flexWrap: 'wrap' }}>
          <Link
            className="button button--lg"
            to="/docs/getting-started/quickstart"
            style={{ background: '#FF3621', border: 'none', color: '#fff', fontWeight: 700 }}
          >
            Get Started →
          </Link>
          <Link
            className="button button--lg button--outline"
            to="/docs/intro"
            style={{ color: '#fff', borderColor: 'rgba(255,255,255,0.5)' }}
          >
            What is SDP-META?
          </Link>
          <Link
            className="button button--lg button--outline"
            href="https://github.com/databrickslabs/sdp-meta"
            style={{ color: '#fff', borderColor: 'rgba(255,255,255,0.5)' }}
          >
            GitHub ↗
          </Link>
        </div>
        <div style={{ marginTop: '1rem' }}>
          <Link
            to="/docs/getting-started/#prefer-video"
            style={{
              color: 'rgba(255,255,255,0.85)',
              fontSize: '0.9rem',
              fontWeight: 600,
              textDecoration: 'none',
            }}
          >
            ▶ Watch the SDP-META deep dive
          </Link>
        </div>
        {/* Badges */}
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '0.5rem',
            justifyContent: 'center',
            marginTop: '2rem',
          }}
        >
          {[
            { label: 'docs', color: '#00A972', value: 'passing' },
            { label: 'pypi', color: '#0075c2', value: 'v0.1.0' },
            {
              label: 'downloads',
              color: '#0075c2',
              value: '225k/mo',
              href: 'https://pepy.tech/projects/dlt-meta',
            },
            { label: 'license', color: '#FF3621', value: 'Databricks Labs' },
          ].map((b) => {
            const badge = (
              <span
                style={{
                  display: 'inline-flex',
                  fontSize: '0.75rem',
                  borderRadius: '4px',
                  overflow: 'hidden',
                }}
              >
                <span
                  style={{
                    background: '#555',
                    color: '#fff',
                    padding: '0.2rem 0.5rem',
                  }}
                >
                  {b.label}
                </span>
                <span
                  style={{
                    background: b.color,
                    color: '#fff',
                    padding: '0.2rem 0.5rem',
                    fontWeight: 600,
                  }}
                >
                  {b.value}
                </span>
              </span>
            );
            return b.href ? (
              <a key={b.label} href={b.href} style={{ textDecoration: 'none' }}>
                {badge}
              </a>
            ) : (
              <span key={b.label}>{badge}</span>
            );
          })}
        </div>
      </div>
    </header>
  );
}

function HowItWorksSection() {
  // Colours
  const C = { dark: '#1B3139', red: '#FF3621', green: '#00A972', grey: '#6b7280', mid: '#374151' };

  return (
    <section style={{ padding: '3.5rem 0', borderBottom: '1px solid var(--ifm-color-emphasis-200)' }}>
      <div className="container" style={{ maxWidth: 900 }}>

        {/* Tagline */}
        <p style={{
          textAlign: 'center', fontSize: '1.05rem',
          color: 'var(--ifm-color-emphasis-700)', marginBottom: '2.5rem', lineHeight: 1.6,
        }}>
          Describe your pipelines in YAML or JSON.{' '}
          <strong>SDP-META generates the entire Lakeflow graph — no pipeline code to write.</strong>
        </p>

        {/* Horizontal scroll on narrow viewports so SVG text stays legible */}
        <div style={{ overflowX: 'auto', WebkitOverflowScrolling: 'touch', marginBottom: '1rem' }}>
        <svg
          viewBox="0 0 910 260"
          xmlns="http://www.w3.org/2000/svg"
          style={{ minWidth: 600, width: '100%', height: 'auto', display: 'block' }}
          aria-label="SDP-META data flow: sources → onboarding YAML → onboard job → DataflowSpec tables → Generic Pipeline → Bronze, Silver, and Quarantine tables"
        >
          <defs>
            <marker id="arr" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
              <path d="M0,0 L0,7 L7,3.5 z" fill={C.grey} />
            </marker>
            {/* Hard-clip YAML code lines to the dark box — prevents overflow on any font */}
            <clipPath id="yamlClip">
              <rect x="162" y="70" width="154" height="168" rx="0" />
            </clipPath>
          </defs>

          {/* ── Sources ── */}
          {[
            { y: 50,  label: 'Autoloader',       sub: 'S3 / ADLS / GCS', small: false },
            { y: 115, label: 'Kafka / EventHub',  sub: '',                small: false },
            { y: 180, label: 'Delta',             sub: 'Bronze → Silver', small: false },
          ].map(({ y, label, sub, small }) => (
            <g key={label}>
              <rect x="0" y={y - 18} width="118" height={small ? 24 : 44} rx="6"
                fill="none" stroke={C.grey} strokeWidth="1.2" />
              <text x="59" y={y + (small ? 0 : 0)} textAnchor="middle"
                fontSize={small ? 9 : 12} fontWeight="600" fill={C.mid}>{label}</text>
              {sub && <text x="59" y={y + 15} textAnchor="middle" fontSize="9.5" fill={C.grey}>{sub}</text>}
            </g>
          ))}
          {[50, 115, 180].map(y => (
            <line key={y} x1="118" y1={y + 4} x2="156" y2={y + 4}
              stroke={C.grey} strokeWidth="1.3" markerEnd="url(#arr)" />
          ))}

          {/* ── onboarding.yaml / .json box ── */}
          {/* width=160 gives 140px text area; at font-size=8 monospace ≈ 4.8px/char → ~29 chars max */}
          <rect x="160" y="20" width="160" height="222" rx="8" fill={C.dark} />
          <text x="240" y="47" textAnchor="middle" fontSize="12" fontWeight="700" fill="#fff">onboarding</text>
          <text x="240" y="62" textAnchor="middle" fontSize="12" fontWeight="700" fill="#fff">.yaml / .json</text>
          <g clipPath="url(#yamlClip)">
            {[
              { t: 'source_format: cloudFiles',  hi: true  },
              { t: 'bronze_table: orders_raw',   hi: false },
              { t: 'silver_table: orders',        hi: true  },
              { t: 'silver_transformation_json',  hi: false },
              { t: 'bronze_data_quality_',        hi: true  },
              { t: '  expectations_json',         hi: false },
              { t: 'bronze_cdc_apply_changes',    hi: true  },
            ].map(({ t, hi }, i) => (
              <text key={i} x="170" y={84 + i * 20} fontSize="8"
                fontFamily="monospace" fill={hi ? '#93c5fd' : '#d1d5db'}>{t}</text>
            ))}
          </g>

          {/* arrow yaml → onboard job */}
          <line x1="320" y1="131" x2="362" y2="131"
            stroke={C.grey} strokeWidth="1.5" markerEnd="url(#arr)" />

          {/* ── Onboarding Job ── */}
          <rect x="364" y="97" width="110" height="68" rx="8" fill={C.green} />
          <text x="419" y="126" textAnchor="middle" fontSize="12" fontWeight="700" fill="#fff">Onboarding</text>
          <text x="419" y="142" textAnchor="middle" fontSize="12" fontWeight="700" fill="#fff">Job</text>
          <text x="419" y="156" textAnchor="middle" fontSize="9" fill="rgba(255,255,255,0.8)">runs once</text>

          {/* arrow onboard → spec tables */}
          <line x1="474" y1="131" x2="510" y2="131"
            stroke={C.grey} strokeWidth="1.5" markerEnd="url(#arr)" />

          {/* ── DataflowSpec tables ── */}
          <rect x="514" y="72" width="138" height="46" rx="6"
            fill="none" stroke={C.dark} strokeWidth="1.5" />
          <text x="583" y="93" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={C.dark}>bronze_dataflowspec</text>
          <text x="583" y="108" textAnchor="middle" fontSize="9" fill={C.grey}>Delta table · Unity Catalog</text>

          <rect x="514" y="126" width="138" height="46" rx="6"
            fill="none" stroke={C.dark} strokeWidth="1.5" />
          <text x="583" y="147" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={C.dark}>silver_dataflowspec</text>
          <text x="583" y="162" textAnchor="middle" fontSize="9" fill={C.grey}>Delta table · Unity Catalog</text>

          {/* arrow spec → pipeline */}
          <line x1="652" y1="131" x2="690" y2="131"
            stroke={C.grey} strokeWidth="1.5" markerEnd="url(#arr)" />

          {/* ── Generic Pipeline ── */}
          <rect x="694" y="82" width="120" height="98" rx="8" fill={C.red} />
          <text x="754" y="110" textAnchor="middle" fontSize="13" fontWeight="700" fill="#fff">Generic</text>
          <text x="754" y="127" textAnchor="middle" fontSize="13" fontWeight="700" fill="#fff">Pipeline</text>
          {['Bronze flows', 'Silver flows', 'DQ · CDC · Sinks'].map((t, i) => (
            <text key={t} x="754" y={147 + i * 14} textAnchor="middle"
              fontSize="9" fill="rgba(255,255,255,0.85)">{'· ' + t}</text>
          ))}

          {/* arrows pipeline → outputs */}
          <line x1="814" y1="107" x2="832" y2="76"  stroke={C.grey} strokeWidth="1.3" markerEnd="url(#arr)" />
          <line x1="814" y1="131" x2="832" y2="131" stroke={C.grey} strokeWidth="1.3" markerEnd="url(#arr)" />
          <line x1="814" y1="155" x2="832" y2="188" stroke={C.grey} strokeWidth="1.3" markerEnd="url(#arr)" />

          {/* ── Output tables ── */}
          {[
            { y: 50,  word1: 'Bronze',     word2: 'tables', fill: '#d97706', outline: false },
            { y: 112, word1: 'Silver',     word2: 'tables', fill: '#4b5563', outline: false },
            { y: 168, word1: 'Quarantine', word2: 'tables', fill: '#9ca3af', outline: true  },
          ].map(({ y, word1, word2, fill, outline }) => (
            <g key={word1}>
              <rect x="836" y={y} width="66" height="38" rx="6"
                fill={outline ? 'none' : fill}
                stroke={outline ? fill : 'none'}
                strokeWidth="1.3" />
              <text x="869" y={y + 15} textAnchor="middle" fontSize="9.5" fontWeight="700"
                fill={outline ? fill : '#fff'}>{word1}</text>
              <text x="869" y={y + 28} textAnchor="middle" fontSize="9"
                fill={outline ? fill : 'rgba(255,255,255,0.85)'}>{word2}</text>
            </g>
          ))}
        </svg>
        </div>{/* end scroll wrapper */}

        <div style={{ textAlign: 'center' }}>
          <Link to="/docs/intro" style={{ fontSize: '0.88rem', fontWeight: 600 }}>
            Full architecture overview →
          </Link>
        </div>
      </div>
    </section>
  );
}

function MigrationNoticeSection() {
  return (
    <section style={{ padding: '0.9rem 0', background: 'var(--ifm-background-surface-color)' }}>
      <div className="container">
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '0.75rem',
            flexWrap: 'wrap',
            border: '1px solid var(--ifm-color-emphasis-200)',
            borderRadius: '999px',
            padding: '0.55rem 1rem',
            background: 'var(--ifm-card-background-color)',
            fontSize: '0.9rem',
          }}
        >
          <span
            style={{
              background: '#FF3621',
              color: '#fff',
              borderRadius: '999px',
              padding: '0.15rem 0.55rem',
              fontSize: '0.72rem',
              fontWeight: 800,
              letterSpacing: '0.04em',
              textTransform: 'uppercase',
            }}
          >
            v0.1.0
          </span>
          <span style={{ color: 'var(--ifm-color-emphasis-800)' }}>
            DLT-META is now SDP-META; existing onboarding files still work.
          </span>
          <Link to="/docs/operations/migration" style={{ fontWeight: 700, whiteSpace: 'nowrap' }}>
            Migration guide →
          </Link>
        </div>
      </div>
    </section>
  );
}

function QuickstartSection() {
  return (
    <section style={{ padding: '3rem 0', background: 'var(--ifm-background-surface-color)' }}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <h2 style={{ fontWeight: 800, fontSize: '1.8rem' }}>Zero to pipeline in minutes</h2>
            <p style={{ color: 'var(--ifm-color-emphasis-700)', marginBottom: '1.5rem' }}>
              The recommended path is a <strong>Declarative Automation Bundle</strong> — git-tracked,
              CI/CD-ready, promotes across dev/prod. One command scaffolds everything.
            </p>
            <Link
              className="button button--primary button--lg"
              to="/docs/getting-started/quickstart"
              style={{ fontWeight: 700 }}
            >
              Full Quickstart Guide →
            </Link>
          </div>
          <div className="col col--6">
            <pre
              style={{
                background: '#1B3139',
                color: '#e2e8f0',
                borderRadius: '8px',
                padding: '1.25rem',
                fontSize: '0.82rem',
                lineHeight: 1.7,
                overflowX: 'auto',
                margin: 0,
              }}
            >
              <code>{quickstart}</code>
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
}

function PathsSection() {
  const paths = [
    {
      emoji: '📦',
      title: 'Declarative Automation Bundles',
      desc: 'Recommended. Git-tracked, CI/CD-ready, multi-target.',
      to: '/docs/getting-started/dabs',
      recommended: true,
    },
    {
      emoji: '⌨️',
      title: 'Interactive CLI',
      desc: 'Quick exploration. onboard + deploy in minutes.',
      to: '/docs/getting-started/cli',
      recommended: false,
    },
    {
      emoji: '🖥️',
      title: 'Databricks App',
      desc: 'GUI for non-engineers. Point-and-click onboarding.',
      to: '/docs/getting-started/app',
      recommended: false,
    },
    {
      emoji: '🤖',
      title: 'MCP Server',
      desc: 'AI-assisted. Let your AI agent scaffold your pipelines.',
      to: '/docs/getting-started/mcp',
      recommended: false,
    },
  ];

  return (
    <section style={{ padding: '3rem 0' }}>
      <div className="container">
        <h2 style={{ textAlign: 'center', fontWeight: 800, fontSize: '1.8rem', marginBottom: '0.5rem' }}>
          Choose your path
        </h2>
        <p style={{ textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem' }}>
          SDP-META supports four deployment interfaces. Start with DABs for any real work.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
            gap: '1rem',
          }}
        >
          {paths.map((p) => (
            <Link
              key={p.title}
              to={p.to}
              style={{
                textDecoration: 'none',
                color: 'inherit',
                display: 'block',
                padding: '1.25rem',
                borderRadius: '8px',
                border: `2px solid ${p.recommended ? '#00A972' : 'var(--ifm-color-emphasis-200)'}`,
                transition: 'box-shadow 0.2s, border-color 0.2s',
              }}
            >
              {p.recommended && (
                <span
                  style={{
                    display: 'inline-block',
                    background: '#00A972',
                    color: '#fff',
                    fontSize: '0.65rem',
                    fontWeight: 700,
                    padding: '0.15rem 0.5rem',
                    borderRadius: '99px',
                    marginBottom: '0.5rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.06em',
                  }}
                >
                  Recommended
                </span>
              )}
              <div style={{ fontSize: '1.8rem', marginBottom: '0.4rem' }}>{p.emoji}</div>
              <div style={{ fontWeight: 700, marginBottom: '0.3rem' }}>{p.title}</div>
              <div style={{ fontSize: '0.85rem', color: 'var(--ifm-color-emphasis-700)' }}>{p.desc}</div>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Metadata-driven framework for automated Bronze and Silver pipelines on Lakeflow Spark Declarative Pipelines"
    >
      <HeroBanner />
      <main>
        <HowItWorksSection />
        <MigrationNoticeSection />
        <QuickstartSection />
        <PathsSection />
      </main>
    </Layout>
  );
}
