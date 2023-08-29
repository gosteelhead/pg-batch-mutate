"use strict";
/**
 * This plugin was sponsored by Sprout LLC. 🙏
 *
 * https://sprout.io
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.PgBatchMutatePlugin = void 0;
const grafast_1 = require("grafast");
/*
 * keyword should probably end in 'ed', e.g. 'archived', 'deleted',
 * 'eradicated', 'unpublished', though 'scheduledForDeletion' is probably okay,
 * as is 'template' or 'draft' - have a read through where it's used and judge
 * for yourself
 */
const PgBatchMutatePlugin = {
    name: `PgBatchMutatePlugin`,
    version: "0.0.0",
    inflection: {},
    schema: {
        hooks: {
            init(_, build) {
                return _;
            },
            GraphQLObjectType_fields(fields, build, context) {
                // Only add the field to the root query type
                if (!context.scope.isRootQuery)
                    return fields;
                // Add a field called `meaningOfLife`
                fields.meaningOfLife = {
                    // It's an integer
                    type: build.graphql.GraphQLInt,
                    // When you call the field, you should always return the number '42'
                    plan() {
                        return (0, grafast_1.constant)(42);
                    },
                };
                return fields;
            },
        },
    },
};
exports.PgBatchMutatePlugin = PgBatchMutatePlugin;
exports.default = PgBatchMutatePlugin;
//# sourceMappingURL=PgBatchMutatePlugin.js.map