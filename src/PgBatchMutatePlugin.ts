import "graphile-config";

import type { PgResource } from "@dataplan/pg";
import { assertExecutableStep, constant, lambda, object, ObjectStep } from "grafast";
import type { GraphQLOutputType } from "grafast/graphql";
import { withPgClientTransaction } from "postgraphile/@dataplan/pg";
import { __InputListStep } from "postgraphile/grafast";
import { GraphQLList } from "postgraphile/graphql";
import { sql, SQL, compile } from "postgraphile/pg-sql2";
import { EXPORTABLE } from "postgraphile/graphile-build";
import { TYPES, listOfCodec } from '@dataplan/pg'

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

const isManyCreateEnabled = (resource: PgResource<any, any, any, any, any>) => {
  return (resource.extensions?.tags ?? {})['mncud'] || (resource.extensions?.tags ?? {})['mnc']
}

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
        ).filter((resource) => isInsertable(build, resource)).filter(isManyCreateEnabled);

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
                              // autoApplyAfterParentApplyPlan: true,
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
                      plan: EXPORTABLE(
                        (constant) =>
                          function plan($mutation: ObjectStep<any>) {
                            return (
                              $mutation.getStepForKey(
                                "clientMutationId",
                                true,
                              ) ?? constant(null)
                            );
                          },
                        [constant],
                      ),
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
                              plan: EXPORTABLE(
                                () =>
                                  function plan(
                                    $result: ObjectStep<any>,
                                  ) {
                                    const $ids = $result.get('ids');
                                    const $rows = resource.find();
                                    $rows.where(sql`${$rows.alias}.id = any(${$rows.placeholder($ids, listOfCodec(TYPES.int))})`)
                                    return $rows
                                  },
                                [],
                              ),
                              
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
        ).filter((resource) => isInsertable(build, resource)).filter(isManyCreateEnabled);
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
                    plan: (parentPlan, args, info) => {

                      const $i = args.getRaw(["input", "mnEvent"]) 



                      const $ids = withPgClientTransaction(executor, $i, async (client, arr) => {

                        const sqlColumns: SQL[] = []
                        const sqlValues: SQL[][] = Array(arr.length).fill([])

                        for(let idx = 0; idx < arr.length; idx++) {
                          
                          const inputRow = arr[idx]

                          for(let [key, value] of Object.entries(resource.codec.attributes)) {

                            const graphqlName = inflection.attribute({
                              attributeName: key,
                              codec: resource.codec
                            })


                            if(idx === 0) {
                              sqlColumns.push(sql.identifier(key))
                            }

                            const dataValue = inputRow[graphqlName]

                            // If the key exists, store the data else store DEFAULT.
                            if (inputRow[graphqlName] !== undefined) {
                              // TODO: This used to use gql2pg in v4, couldn't find the equivalent.

                              sqlValues[idx] = [...sqlValues[idx], sql.value(dataValue)]
                            } else {
                              sqlValues[idx] = [...sqlValues[idx], sql.raw('default')]
                            }
                              
                          }

                        }



                        const mutationQuery = sql.query`
                            INSERT INTO ${resource.codec.sqlType} 
                            ${
                              sqlColumns.length
                                ? sql.fragment`(${sql.join(sqlColumns, ', ')})
                              VALUES (${sql.join(
                                sqlValues.map((dataGroup) => sql.fragment`${sql.join(dataGroup, ', ')}`),
                                '),('
                              )})`
                                : sql.fragment`default values`
                            } returning *`




                        const compiled = compile(mutationQuery)


                        const result = await client.query({ text: compiled.text, values: compiled.values });

                        const ids = result.rows.map(row => (row as any).id);

                        return ids
                      })

                      // This is a step representing an object `{ ids: [...] }` that we use to represent the result of our mutation. In the payload we'll expand on what these ids are.
                      const $result = object({ ids: $ids }); // < IMPORTANT: this is `import { object } from 'grafast';`, not the `object` you had as your first arg.
                      return $result;
                    }

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