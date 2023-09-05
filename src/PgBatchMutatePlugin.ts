import "graphile-config";

import type { PgResource } from "@dataplan/pg";
import { assertExecutableStep, constant, lambda, object, ObjectStep } from "grafast";
import type { GraphQLOutputType } from "grafast/graphql";
import { withPgClientTransaction, PgResourceUnique } from "postgraphile/@dataplan/pg";
import { __InputListStep } from "postgraphile/grafast";
import { GraphQLList, GraphQLInt } from "postgraphile/graphql";
import { sql, SQL, compile } from "postgraphile/pg-sql2";
import { EXPORTABLE } from "postgraphile/graphile-build";
import { TYPES, listOfCodec, PgCodecWithAttributes } from '@dataplan/pg'

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
      // Create
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
      // Update
      mnUpdateField(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
      mnUpdateByKeysField(
        this: Inflection,
        details: {
          resource: PgResource<any, any, any, any, any>;
          unique: PgResourceUnique;
        },
      ): string;

      mnUpdateInputType(
        this: Inflection,
        details: {
          resource: PgResource<any, any, any, any, any>;
          unique: PgResourceUnique;
        },
      ): string;
      mnUpdatePayloadType(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;

      // Delete
      mnDeleteField(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
      mnDeleteByKeysField(
        this: Inflection,
        details: {
          resource: PgResource<any, any, any, any, any>;
          unique: PgResourceUnique;
        },
      ): string;

      mnDeleteInputType(
        this: Inflection,
        details: {
          resource: PgResource<any, any, any, any, any>;
          unique: PgResourceUnique;
        },
      ): string;
      mnDeletePayloadType(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;
      mnTableIdsPayload(
        this: Inflection,
        resource: PgResource<any, any, any, any, any>,
      ): string;


      mnTableFieldNamePatch(
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

function getSpecs(
  build: GraphileBuild.Build,
  resource: PgResource<any, PgCodecWithAttributes, any, any, any>,
  mode: "resource:update" | "resource:delete",
) {
  const primaryUnique = resource.uniques.find(
    (u: PgResourceUnique) => u.isPrimary,
  );
  const constraintMode = `constraint:${mode}`;
  const specs: { unique: PgResourceUnique, uniqueMode: string}[] = [
    ...(primaryUnique &&
    build.getNodeIdCodec !== undefined &&
    build.behavior.pgCodecMatches(resource.codec, `nodeId:${mode}`)
      ? [{ unique: primaryUnique, uniqueMode: "node" }]
      : []),
    ...resource.uniques
      .filter((unique: PgResourceUnique) => {
        return build.behavior.pgResourceUniqueMatches(
          [resource, unique],
          constraintMode,
        );
      })
      .map((unique: PgResourceUnique) => ({
        unique,
        uniqueMode: "keys",
      })),
  ];
  return specs;
}

export const PgBatchMutatePlugin: GraphileConfig.Plugin = {
  name: "PgMutationBatchCreatePlugin",
  description: "Adds 'BatchCreate' mutation for supported table-like sources",
  version: "0.0.0",
  after: ["smart-tags"],

  inflection: {
    add: {
      // Create
      mnCreateField(options, resource) {
        return this.camelCase(`mn-create-${this.tableType(resource.codec)}`);
      },
      mnCreateInputType(options, resource) {
        return this.upperCamelCase(`${this.mnCreateField(resource)}-input`);
      },
      mnCreatePayloadType(options, resource) {
        return this.upperCamelCase(`${this.mnCreateField(resource)}-payload`);
      },

      // Update
      mnUpdateField(options, resource) {
        return this.camelCase(`mn-update-${this.tableType(resource.codec)}`);
      },

      mnUpdateByKeysField(options, { resource, unique }) {
        return this.camelCase(
          `mn-update-${this._singularizedResourceName(
            resource,
          )}-by-${this._joinAttributeNames(resource.codec, unique.attributes)}`,
        );
      },
      mnTableFieldNamePatch(options, resource) {
        return this.camelCase(`mn-${this.tableType(resource.codec)}-patch`);
      },

      mnUpdateInputType(options, details) {
        return this.upperCamelCase(`${this.mnUpdateByKeysField(details)}-input`);
      },
      mnUpdatePayloadType(options, resource) {
        return this.upperCamelCase(`${this.mnUpdateField(resource)}-payload`);
      },

      // Delete
      mnDeleteField(options, resource) {
        return this.camelCase(`mn-delete-${this.tableType(resource.codec)}`);
      },
      mnDeleteByKeysField(options, { resource, unique }) {
        return this.camelCase(
          `mn-delete-${this._singularizedResourceName(
            resource,
          )}-by-${this._joinAttributeNames(resource.codec, unique.attributes)}`,
        );
      },

      mnDeleteInputType(options, details) {
        return this.upperCamelCase(`${this.mnDeleteByKeysField(details)}-input`);
      },
      mnDeletePayloadType(options, resource) {
        return this.upperCamelCase(`${this.mnDeleteField(resource)}-payload`);
      },

      mnTableIdsPayload(options, resource) {
        return this.camelCase(`deleted-${this.tableType(resource.codec)}-ids`);
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

        // Create
        insertableResources.filter(isManyCreateEnabled).forEach((resource) => {

          build.recoverable(null, () => {
            const tableTypeName = inflection.tableType(resource.codec);
            const tableFieldNamePayload = inflection.mnTableFieldNamePayload(resource);
            


            // Create
            if(isManyCreateEnabled(resource)) {

              const tableFieldNameInput = inflection.mnTableFieldNameInput(resource);
              const inputTypeName = inflection.mnCreateInputType(resource);
              const payloadTypeName = inflection.mnCreatePayloadType(resource);

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
            }


            // Update
            if(true) {

              const tableFieldNamePatch = inflection.mnTableFieldNamePatch(resource);

              const specs = getSpecs(build, resource, 'resource:update');
              for(const spec of specs) {
                const { uniqueMode, unique } = spec;
                const details = {
                  resource,
                  unique,
                };
                if (uniqueMode === "node" && !build.getNodeIdHandler) {
                  continue;
                }

                const inputTypeName = inflection.mnUpdateInputType(details);
                const payloadTypeName = inflection.mnUpdatePayloadType(resource);

                if(spec.uniqueMode !== 'keys') {
                  continue
                }

                build.registerInputObjectType(
                  inputTypeName,
                  { isMutationInput: true },
                  () => ({
                    description: `All input for the many update \`${tableTypeName}\` mutation.`,
                    fields: ({ fieldWithHooks }) => {
                      const TableInput = build.getGraphQLTypeByPgCodec(
                        resource.codec,
                        "patch",
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
                              [tableFieldNamePatch]: fieldWithHooks(
                                {
                                  fieldName: tableFieldNamePatch,
                                  fieldBehaviorScope: `update:input:record`,
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

                build.registerObjectType(
                  payloadTypeName,
                  {
                    isMutationPayload: true,
                    // isPgCreatePayloadType: true,
                    pgTypeResource: resource,
                  },
                  () => ({
                    assertStep: assertExecutableStep,
                    description: `The output of our many update \`${tableTypeName}\` mutation.`,
                    fields: ({ fieldWithHooks }) => {
                      const TableType = build.getGraphQLTypeByPgCodec(
                        resource.codec,
                        "output",
                      ) as GraphQLOutputType | undefined;
                      const fieldBehaviorScope = `insert:resource:update`;
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
                                  description: `The \`${tableTypeName}\`s that was updated by this mutation.`,
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

              }

              

            }

            // Delete
            if(true) {

              const tableFieldNamePatch = inflection.mnTableFieldNamePatch(resource);
              const deletedIdsName = inflection.mnTableIdsPayload(resource)

              const specs = getSpecs(build, resource, 'resource:delete');
              for(const spec of specs) {
                const { uniqueMode, unique } = spec;
                const details = {
                  resource,
                  unique,
                };
                if (uniqueMode === "node" && !build.getNodeIdHandler) {
                  continue;
                }

                const inputTypeName = inflection.mnDeleteInputType(details);
                const payloadTypeName = inflection.mnDeletePayloadType(resource);

                if(spec.uniqueMode !== 'keys') {
                  continue
                }


                build.registerInputObjectType(
                  inputTypeName,
                  { isMutationInput: true },
                  () => ({
                    description: `All input for the many delete \`${tableTypeName}\` mutation.`,
                    fields: ({ fieldWithHooks }) => {
                      const TableInput = build.getGraphQLTypeByPgCodec(
                        resource.codec,
                        "patch",
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
                              [tableFieldNamePatch]: fieldWithHooks(
                                {
                                  fieldName: tableFieldNamePatch,
                                  fieldBehaviorScope: `delete:input:record`,
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
                  `PgMutationDeletePlugin input for ${resource.name}`,
                );

                build.registerObjectType(
                  payloadTypeName,
                  {
                    isMutationPayload: true,
                    // isPgCreatePayloadType: true,
                    pgTypeResource: resource,
                  },
                  () => ({
                    assertStep: assertExecutableStep,
                    description: `The output of our many delete \`${tableTypeName}\` mutation.`,
                    fields: ({ fieldWithHooks }) => {
                      const TableType = build.getGraphQLTypeByPgCodec(
                        resource.codec,
                        "output",
                      ) as GraphQLOutputType | undefined;
                      const fieldBehaviorScope = `insert:resource:delete`;
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
                              [deletedIdsName]: fieldWithHooks(
                                {
                                  fieldName: deletedIdsName,
                                  fieldBehaviorScope,
                                },
                                {
                                  description: `The \`${tableTypeName}\`s that was deleted by this mutation.`,
                                  type: new GraphQLList(new GraphQLNonNull(GraphQLInt)),
                                  plan: EXPORTABLE(
                                    () =>
                                      function plan(
                                        $result: ObjectStep<any>,
                                      ) {
                                        const $ids = $result.get('ids');
                                        return $ids
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
                  `PgMutationDeletePlugin payload for ${resource.name}`,
                );

              }

              

            }


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
        
        const createFunctionality = insertableSources.filter(isManyCreateEnabled).reduce((memo, resource) => {
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



        // Update
        for(const resource of insertableSources.filter(isManyCreateEnabled)) {

          for(const spec of getSpecs(build, resource, 'resource:update').filter(s => s.uniqueMode === 'keys')) {
            const createFieldName = inflection.mnUpdateField(resource);
            
            const payloadTypeName = inflection.mnUpdatePayloadType(resource);
            const payloadType = build.getOutputTypeByName(payloadTypeName);

            const spec = getSpecs(build, resource, 'resource:update').find(s => s.uniqueMode === 'keys');

            if(!spec) {
              continue
            }

            const { unique } = spec;
            const details = {
              resource,
              unique,
            };


            const mutationInputType = build.getInputTypeByName(
              inflection.mnUpdateInputType(details),
            );



            const {executor} = resource
            
            build.recoverable(fields, () => {
              return build.extend(fields, 
                {
                  [createFieldName]: fieldWithHooks(
                    {
                      fieldName: createFieldName,
                      fieldBehaviorScope: "resource:update",
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
                      description: `Updates many \`${inflection.tableType(
                        resource.codec,
                      )}\`.`,
                      deprecationReason: tagToString(
                        resource.extensions?.tags?.deprecated,
                      ),
                      plan: (parentPlan, args, info) => {
  
                        const $i = args.getRaw(["input", "mnEventPatch"]) 
  
  
                        const $ids = withPgClientTransaction(executor, $i, async (client, arr) => {
  
                          const sqlColumns: SQL[] = []
                          const sqlColumnTypes: SQL[] = []
                          const allSQLColumns: SQL[] = []
                          const inputData: Object[] = arr
                          if (!inputData || inputData.length === 0) return null
                          
                          const sqlValues: SQL[][] = Array(inputData.length).fill([])
  
                          const usedSQLColumns: SQL[] = []
                          const usedColSQLVals: SQL[][] = Array(inputData.length).fill([])
                          let hasConstraintValue = true
  
                          inputData.forEach((dataObj, i) => {
                            let setOfRcvdDataHasPKValue = false
  
                            for(let [key, value] of Object.entries(resource.codec.attributes)) {
  
                              const fieldName = inflection.attribute({
                                attributeName: key,
                                codec: resource.codec
                              })
                              const dataValue = (dataObj as any)[fieldName]

  
                              const isConstraintAttr = spec.unique.attributes.some((att) => att === key)

  
                              // Store all attributes on the first run.
                              // Skip the primary keys, since we can't update those.
                              if (i === 0 && !isConstraintAttr) {
                                sqlColumns.push(sql.raw(key))
                                usedSQLColumns.push(sql.raw('use_' + key))
                                // Handle custom types
                                
                                // TODO: Do we handle Postgraphile enums correctly? Or custom types? Like steelhead.email.
                                // if (attr.type.namespaceName !== 'pg_catalog') {
                                //   if (attr.type.isFake && attr.type.id.startsWith('FAKE_ENUM_steelhead_')) {
                                //     sqlColumnTypes.push(sql.raw('text')) // Benjie: (https://github.com/graphile/postgraphile/issues/1365)
                                //   } else {
                                //     sqlColumnTypes.push(sql.raw(attr.class.namespaceName + '.' + attr.type.name))
                                //   }
                                // } else {
                                //   sqlColumnTypes.push(value.codec.sqlType)
                                // }

                                sqlColumnTypes.push(value.codec.sqlType)

                              }
                              // Get all of the attributes
                              if (i === 0) {
                                allSQLColumns.push(sql.raw(key))
                              }
                              // Push the data value if it exists, else push
                              // a dummy null value (which will not be used).
                              if (fieldName in dataObj && dataValue !== undefined) {
                                sqlValues[i] = [...sqlValues[i], sql.value(dataValue)]
                                if (!isConstraintAttr) {
                                  usedColSQLVals[i] = [...usedColSQLVals[i], sql.raw('true')]
                                } else {
                                  setOfRcvdDataHasPKValue = true
                                }
                              } else {
                                sqlValues[i] = [...sqlValues[i], sql.raw('NULL')]
                                if (!isConstraintAttr) {
                                  usedColSQLVals[i] = [...usedColSQLVals[i], sql.raw('false')]
                                }
                              }

                            }
                            if (!setOfRcvdDataHasPKValue) {
                              hasConstraintValue = false
                            }
                          })
  
                          if (!hasConstraintValue) {
                            throw new Error(
                              `You must provide the primary key(s) in the updated data for updates on '${inflection.pluralize(
                                inflection.singularize(resource.name)
                              )}'`
                            )
                          }
  
                          if (sqlColumns.length === 0) return null
  
                          // https://stackoverflow.com/questions/63290696/update-multiple-rows-using-postgresql
                          const mutationQuery = sql.query`\ 
                          UPDATE ${resource.codec.sqlType} t1 SET
                            ${sql.join(
                              sqlColumns.map(
                                (col, i) =>
                                  sql.fragment`"${col}" = (CASE WHEN t2."use_${col}" THEN t2."${col}"::${sqlColumnTypes[i]} ELSE t1."${col}" END)`
                              ),
                              ', '
                            )}
                          FROM (VALUES
                                (${sql.join(
                                  sqlValues.map((dataGroup, i) => sql.fragment`${sql.join(dataGroup.concat(usedColSQLVals[i]), ', ')}`),
                                  '),('
                                )})
                              ) t2(
                                ${sql.join(
                                  allSQLColumns
                                    .map((col) => sql.fragment`"${col}"`)
                                    .concat(usedSQLColumns.map((useCol) => sql.fragment`"${useCol}"`)),
                                  ', '
                                )}
                              )
                          WHERE ${sql.fragment`(${sql.join(
                            spec.unique.attributes.map(
                              (key) =>
                                sql.fragment`t2.${sql.identifier(key)}::${resource.codec.attributes[key].codec.sqlType} = t1.${sql.identifier(key)}`
                            ),
                            ') and ('
                          )})`}
                          RETURNING ${sql.join(
                            allSQLColumns.map((col) => sql.fragment`t1."${col}"`),
                            ', '
                          )}
                          `

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
                `doing cool stuff in code`)
            })
          }

          

        }

        // Delete
        for(const resource of insertableSources.filter(isManyCreateEnabled)) {

          for(const spec of getSpecs(build, resource, 'resource:delete').filter(s => s.uniqueMode === 'keys')) {
            const createFieldName = inflection.mnDeleteField(resource);
            
            const payloadTypeName = inflection.mnDeletePayloadType(resource);
            const payloadType = build.getOutputTypeByName(payloadTypeName);

            const spec = getSpecs(build, resource, 'resource:delete').find(s => s.uniqueMode === 'keys');

            if(!spec) {
              continue
            }

            const { unique } = spec;
            const details = {
              resource,
              unique,
            };


            const mutationInputType = build.getInputTypeByName(
              inflection.mnDeleteInputType(details),
            );



            const {executor} = resource
            
            build.recoverable(fields, () => {
              return build.extend(fields, 
                {
                  [createFieldName]: fieldWithHooks(
                    {
                      fieldName: createFieldName,
                      fieldBehaviorScope: "resource:delete",
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
                      description: `Deletes many \`${inflection.tableType(
                        resource.codec,
                      )}\`.`,
                      deprecationReason: tagToString(
                        resource.extensions?.tags?.deprecated,
                      ),
                      plan: (parentPlan, args, info) => {
  
                        const $i = args.getRaw(["input", "mnEventPatch"]) 
  
  
                        const $ids = withPgClientTransaction(executor, $i, async (client, arr) => {
  
                          const sqlColumns: SQL[] = []
                        const inputData: Object[] = arr
                        if (!inputData || inputData.length === 0) return null
                        const sqlValues: SQL[][] = Array(inputData.length).fill([])
                        let hasConstraintValue = true

                        inputData.forEach((dataObj, i) => {
                          let setOfRcvdDataHasPKValue = false

                          for(let [key, value] of Object.entries(resource.codec.attributes)) {
  
                            const fieldName = inflection.attribute({
                              attributeName: key,
                              codec: resource.codec
                            })
                            const dataValue = (dataObj as any)[fieldName]


                            const isConstraintAttr = spec.unique.attributes.some((att) => att === key)



                            // Ensure that the field values are PKs since that's
                            // all we care about for deletions.
                            if (!isConstraintAttr) return
                            // Store all attributes on the first run.
                            if (i === 0) {
                              sqlColumns.push(sql.raw(key))
                            }
                            if (fieldName in dataObj) {
                              sqlValues[i] = [...sqlValues[i], sql.value(dataValue)]
                              if (isConstraintAttr) {
                                setOfRcvdDataHasPKValue = true
                              }
                            }

                          }

                          if (!setOfRcvdDataHasPKValue) {
                            hasConstraintValue = false
                          }
                        })

                        if (!hasConstraintValue) {
                          throw new Error(
                            `You must provide the primary key(s) in the provided data for deletes on '${inflection.pluralize(
                              inflection.singularize(resource.name)
                            )}'`
                          )
                        }

                        if (sqlColumns.length === 0) return null

                        const mutationQuery = sql.query`\
                          DELETE FROM ${resource.codec.sqlType}
                          WHERE
                            (${sql.join(
                              sqlValues.map(
                                (dataGroup, i) =>
                                  sql.fragment`(${sql.join(
                                    dataGroup.map((val, j) => sql.fragment`"${sqlColumns[j]}" = ${val}`),
                                    ') and ('
                                  )})`
                              ),
                              ') or ('
                            )})
                          RETURNING *
                        `
  

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
                `doing cool stuff in code`)
            })
          }

          

        }




        return build.extend(createFunctionality, {}, `combining crud functionality.`)
      },
    },
  },
};