// @ts-check
const { themes: prismThemes } = require('prism-react-renderer');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'SDP-META',
  tagline: 'Metadata-driven framework for automated Lakeflow Spark Declarative Pipelines',
  favicon: 'img/favicon.ico',

  url: 'https://databrickslabs.github.io',
  baseUrl: '/sdp-meta/',

  organizationName: 'databrickslabs',
  projectName: 'sdp-meta',

  onBrokenLinks: 'warn',

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  themes: ['@docusaurus/theme-mermaid'],

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/databrickslabs/sdp-meta/tree/main/docs/docs/',
          showLastUpdateTime: true,
          showLastUpdateAuthor: true,
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/sdp-meta-banner.png',
      navbar: {
        title: 'SDP-META',
        logo: {
          alt: 'SDP-META Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            to: '/docs/reference',
            label: 'Reference',
            position: 'left',
          },
          {
            to: '/docs/changelog',
            label: 'Changelog',
            position: 'left',
          },
          {
            href: 'https://github.com/databrickslabs/sdp-meta',
            label: 'GitHub',
            position: 'right',
          },
          {
            href: 'https://pypi.org/project/databricks-labs-sdp-meta/',
            label: 'PyPI',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              { label: 'Getting Started', to: '/docs/getting-started' },
              { label: 'Concepts', to: '/docs/concepts' },
              { label: 'Reference', to: '/docs/reference' },
              { label: 'Guides', to: '/docs/guides' },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'GitHub Issues',
                href: 'https://github.com/databrickslabs/sdp-meta/issues',
              },
              {
                label: 'Databricks Labs',
                href: 'https://github.com/databrickslabs',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'PyPI',
                href: 'https://pypi.org/project/databricks-labs-sdp-meta/',
              },
              {
                label: 'Changelog',
                to: '/docs/changelog',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Databricks Labs.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['bash', 'json', 'yaml', 'python', 'sql'],
      },
      colorMode: {
        defaultMode: 'light',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
    }),
};

module.exports = config;
