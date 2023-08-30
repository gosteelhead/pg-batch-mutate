import "graphile-config";

import type { PgResource } from "@dataplan/pg";
import { assertExecutableStep, constant } from "grafast";
import type { GraphQLOutputType } from "grafast/graphql";
import { withPgClientTransaction } from "postgraphile/@dataplan/pg";
import { __InputListStep } from "postgraphile/grafast";
import { GraphQLList } from "postgraphile/graphql";

export function tagToString(
    str: undefined | null | boolean | string | (string | boolean)[],
  ): string | undefined {
    if (!str || (Array.isArray(str) && str.length === 0)) {
      return undefined;
    }
    return Array.isArray(str) ? str.join("\n") : str === true ? " " : str;
  }

declare global {
  namespace GraphileBuild {
    // interface ScopeObject {
    //   isPgCreatePayloadType?: boolean;
    // }
    interface Inflection {
      mnCreateField(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
      mnCreateInputType(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
      mnCreatePayloadType(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
      mnTableFieldNameInput(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
      mnTableFieldNamePayload(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
    }
  }
}

const isInsertable = (
  build: GraphileBuild.Build,
  resource: PgResource<any, any, any, any, any>,
) => {
  if (resource.parameters) return false;
  if (!resource.codec.attributes) return false;
  if (resource.codec.polymorphism) return false;
  if (resource.codec.isAnonymous) return false;
  return build.behavior.pgResourceMatches(resource, "resource:insert") === true;
};

export const PgBatchMutatePlugin: GraphileConfig.Plugin = {
  name: "PgMutationBatchCreatePlugin",
  description: "Adds 'BatchCreate' mutation for supported table-like sources",
  version: "0.0.0",
  after: ["smart-tags"],

  inflection: {
    add: {
      mnCreateField(options, resource) {
        return this.camelCase(`mn-create-${this.tableType(resource.codec)}`);
      },
      mnCreateInputType(options, resource) {
        return this.upperCamelCase(`${this.mnCreateField(resource)}-input`);
      },
      mnCreatePayloadType(options, resource) {
        return this.upperCamelCase(`${this.mnCreateField(resource)}-payload`);
      },
      mnTableFieldNameInput(options, resource) {
        return this.camelCase(`mn-${this.tableType(resource.codec)}`);
      },
      mnTableFieldNamePayload(options, resource) {
        return this.pluralize(this.camelCase(`${this.tableType(resource.codec)}`));
      },
    },
  },

  schema: {
    // entityBehavior: {
    //   pgResource: {
    //     provides: ["default"],
    //     before: ["inferred", "override"],
    //     callback(behavior, resource) {
    //       const newBehavior = [behavior, "+insert:resource:select"];
    //       if (
    //         !resource.parameters &&
    //         !!resource.codec.attributes &&
    //         !resource.codec.polymorphism &&
    //         !resource.codec.isAnonymous
    //       ) {
    //         newBehavior.unshift("insert");
    //         newBehavior.unshift("record");
    //       }
    //       return newBehavior;
    //     },
    //   },
    // },
    hooks: {
      init(_, build) {
        const {
          inflection,
          graphql: { GraphQLString, GraphQLNonNull },
        } = build;
        const insertableResources = Object.values(
          build.input.pgRegistry.pgResources,
        ).filter((resource) => isInsertable(build, resource));

        insertableResources.forEach((resource) => {
          build.recoverable(null, () => {
            const tableTypeName = inflection.tableType(resource.codec);
            const inputTypeName = inflection.mnCreateInputType(resource);
            const tableFieldNameInput = inflection.mnTableFieldNameInput(resource);
            const tableFieldNamePayload = inflection.mnTableFieldNamePayload(resource);



            build.registerInputObjectType(
              inputTypeName,
              { isMutationInput: true },
              () => ({
                description: `All input for the many create \`${tableTypeName}\` mutation.`,
                fields: ({ fieldWithHooks }) => {
                  const TableInput = build.getGraphQLTypeByPgCodec(
                    resource.codec,
                    "input",
                  );
                  return {
                    clientMutationId: {
                      type: GraphQLString,
                      // autoApplyAfterParentApplyPlan: true,
                      // applyPlan: EXPORTABLE(
                      //   () =>
                      //     function plan($input: ObjectStep<any>, val) {
                      //       $input.set("clientMutationId", val.get());
                      //     },
                      //   [],
                      // ),
                    },
                    ...(TableInput
                      ? {
                          [tableFieldNameInput]: fieldWithHooks(
                            {
                              fieldName: tableFieldNameInput,
                              fieldBehaviorScope: `insert:input:record`,
                            },
                            () => ({
                              description: build.wrapDescription(
                                `The \`${tableTypeName}\` to be created by this mutation.`,
                                "field",
                              ),
                              type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(TableInput))),
                              autoApplyAfterParentApplyPlan: true,
                              // applyPlan: EXPORTABLE(
                              //   () =>
                              //     function plan(
                              //       $object: ObjectStep<{
                              //         result: PgInsertSingleStep;
                              //       }>,
                              //     ) {
                              //       const $record =
                              //         $object.getStepForKey("result");
                              //       return $record.setPlan();
                              //     },
                              //   [],
                              // ),
                            }),
                          ),
                        }
                      : null),
                  };
                },
              }),
              `PgMutationCreatePlugin input for ${resource.name}`,
            );

            const payloadTypeName = inflection.mnCreatePayloadType(resource);

            build.registerObjectType(
              payloadTypeName,
              {
                isMutationPayload: true,
                // isPgCreatePayloadType: true,
                pgTypeResource: resource,
              },
              () => ({
                assertStep: assertExecutableStep,
                description: `The output of our many create \`${tableTypeName}\` mutation.`,
                fields: ({ fieldWithHooks }) => {
                  const TableType = build.getGraphQLTypeByPgCodec(
                    resource.codec,
                    "output",
                  ) as GraphQLOutputType | undefined;
                  const fieldBehaviorScope = `insert:resource:select`;
                  return {
                    clientMutationId: {
                      type: GraphQLString,
                      // plan: EXPORTABLE(
                      //   (constant) =>
                      //     function plan($mutation: ObjectStep<any>) {
                      //       return (
                      //         $mutation.getStepForKey(
                      //           "clientMutationId",
                      //           true,
                      //         ) ?? constant(null)
                      //       );
                      //     },
                      //   [constant],
                      // ),
                    },
                    ...(TableType &&
                    build.behavior.pgResourceMatches(
                      resource,
                      fieldBehaviorScope,
                    )
                      ? {
                          [tableFieldNamePayload]: fieldWithHooks(
                            {
                              fieldName: tableFieldNamePayload,
                              fieldBehaviorScope,
                            },
                            {
                              description: `The \`${tableTypeName}\`s that was created by this mutation.`,
                              type: new GraphQLList(TableType),
                              // plan: EXPORTABLE(
                              //   () =>
                              //     function plan(
                              //       $object: ObjectStep<{
                              //         result: PgInsertSingleStep;
                              //       }>,
                              //     ) {
                              //       return $object.get("result");
                              //     },
                              //   [],
                              // ),
                              deprecationReason: tagToString(
                                resource.extensions?.tags?.deprecated,
                              ),
                            },
                          ),
                        }
                      : null),
                  };
                },
              }),
              `PgMutationCreatePlugin payload for ${resource.name}`,
            );
          });
        });

        return _;
      },

      GraphQLObjectType_fields(fields, build, context) {
        const {
          inflection,
          graphql: { GraphQLNonNull },
        } = build;
        const {
          scope: { isRootMutation },
          fieldWithHooks,
        } = context;
        if (!isRootMutation) {
          return fields;
        }

        const insertableSources = Object.values(
          build.input.pgRegistry.pgResources,
        ).filter((resource) => isInsertable(build, resource));
        return insertableSources.reduce((memo, resource) => {
          return build.recoverable(memo, () => {
            const createFieldName = inflection.mnCreateField(resource);
            
            const payloadTypeName = inflection.mnCreatePayloadType(resource);
            const payloadType = build.getOutputTypeByName(payloadTypeName);
            const mutationInputType = build.getInputTypeByName(
              inflection.mnCreateInputType(resource),
            );

            const {executor} = resource


            return build.extend(
              memo,
              {
                [createFieldName]: fieldWithHooks(
                  {
                    fieldName: createFieldName,
                    fieldBehaviorScope: "resource:insert",
                  },
                  {
                    args: {
                      input: {
                        type: new GraphQLNonNull(mutationInputType),
                        // autoApplyAfterParentPlan: true,
                        // applyPlan: EXPORTABLE(
                        //   () =>
                        //     function plan(
                        //       _: any,
                        //       $object: ObjectStep<{
                        //         result: PgInsertSingleStep;
                        //       }>,
                        //     ) {
                        //       return $object;
                        //     },
                        //   [],
                        // ),
                      },
                    },
                    type: payloadType,
                    description: `Creates many \`${inflection.tableType(
                      resource.codec,
                    )}\`.`,
                    deprecationReason: tagToString(
                      resource.extensions?.tags?.deprecated,
                    ),
                    // resolve: (source, args, context, info) => {
                    //   console.log(args)
                    // },
                    plan: (object, args, info) => {
                      console.log(object)
                      const i = args.getRaw(["input", "mnEvent"]) as __InputListStep
                      console.log(i.at(0))
                      console.log('args: ', args.getRaw(["input", "mnEvent"]))
                      // for(const el of args.getRaw(["input", "mnEvent"])) {

                      // }
                      return withPgClientTransaction(executor, constant(null), async (client) => {
                        const result = await client.query({ text: `select 1` });
                        return result
                      })
                    }
                    // plan: EXPORTABLE(
                    //   (object, pgInsertSingle, resource) =>
                    //     function plan(_: any, args: FieldArgs) {
                    //       const plan = object({
                    //         result: pgInsertSingle(
                    //           resource,
                    //           Object.create(null),
                    //         ),
                    //       });
                    //       args.apply(plan);
                    //       return plan;
                    //     },
                    //   [object, pgInsertSingle, resource],
                    // ),
                  },
                ),
              },
              `Adding create mutation for ${resource.name}`,
            );
          });
        }, fields);
      },
    },
  },
};