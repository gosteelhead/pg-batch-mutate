/**
 * This plugin was sponsored by Sprout LLC. 🙏
 *
 * https://sprout.io
 */
declare global {
    namespace GraphileBuild {
        interface Inflection {
        }
    }
}
declare module "graphile-build-pg" {
    interface PgCodecRelationTags {
        archivedRelation?: boolean;
    }
}
declare const PgBatchMutatePlugin: GraphileConfig.Plugin;
export default PgBatchMutatePlugin;
export { PgBatchMutatePlugin };
//# sourceMappingURL=PgBatchMutatePlugin.d.ts.map