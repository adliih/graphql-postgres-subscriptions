# [3.0.0](https://github.com/adliih/graphql-postgres-subscriptions/compare/v2.0.0...v3.0.0) (2026-04-14)


* feat!: TypeScript and graphql-subscriptions v3 ([53aae58](https://github.com/adliih/graphql-postgres-subscriptions/commit/53aae58ed71e075bfa60d79f8d0b345f56fa81de))


### Bug Fixes

* **ci:** stabilize semantic-release GitHub fail step and npm auth env ([b7728f6](https://github.com/adliih/graphql-postgres-subscriptions/commit/b7728f6bb90ea3d6f7fc10d3777dc2fcb12738c5))


### BREAKING CHANGES

* Publish as @adliihjs/graphql-postgres-subscriptions with compiled dist/, graphql-subscriptions v3, and TypeScript typings. Remove legacy index.js and postgres-pubsub.js entrypoints; import from the package main/types. Requires Node 14+ and graphql as a peer dependency.
