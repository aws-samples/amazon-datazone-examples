GET_BUSINESS_TERMS_QUERY = """
query Assets {
    assets(
        limit: 50
        order: { id: asc }
        where: {
            type: { publicId: { eq: "BusinessTerm" } }
        }
    ) {
        id
        fullName
        displayName
        stringAttributes {
            stringValue
        }
    }
}
"""

GET_BUSINESS_TERMS_WITH_CURSOR_QUERY = """
query Assets($lastSeenId: UUID!) {
    assets(
        limit: 50
        order: { id: asc }
        where: {
            type: { publicId: { eq: "BusinessTerm" } }
            id: {gt: $lastSeenId}
        }
    ) {
        id
        fullName
        displayName
        stringAttributes {
            stringValue
        }
    }
}
"""

GET_AWS_TABLE_ASSETS_QUERY = """
query Assets {
    assets(
        limit: 10
        order: { id: asc }
        where: {
            type: { publicId: { eq: "Table" } }
            fullName: { startsWith: "AWS", notContains: ">pg_" }
            stringAttributes: {
                any: { type: { name: { eq: "AWS Resource Metadata" } } }
            }
        }
    ) {
        id
        fullName
        displayName
        stringAttributes(where: { type: { name: { eq: "AWS Resource Metadata" } } }) {
            stringValue
            type {
                name
            }
        }
    }
}
"""

GET_AWS_TABLE_ASSETS_WITH_CURSOR_QUERY = """
query Assets($lastSeenId: UUID!) {
    assets(
        limit: 10
        order: { id: asc }
        where: {
            type: { publicId: { eq: "Table" } }
            id: {gt: $lastSeenId}
            fullName: {
                startsWith: "AWS"
                notContains: ">pg_"
            }
            stringAttributes: {
                any: { type: { name: { eq: "AWS Resource Metadata" } } }
            }
        }
    ) {
        id
        fullName
        displayName
        stringAttributes(where: { type: { name: { eq: "AWS Resource Metadata" } } }) {
            stringValue
            type {
                name
            }
        }
    }
}
"""

GET_AWS_TABLE_ASSET_QUERY = """
query Assets($assetId: UUID!) {
    assets(
        limit: 1
        where: {
            type: { publicId: { eq: "Table" } }
            id: { eq: $assetId }
        }
    ) {
        id
        fullName
        displayName
        stringAttributes(where: { type: { publicId: { eq: "Description" } } }) {
            id
            stringValue
        }
        incomingRelations(limit: 1000, where: { source: {type: { publicId: { eq: "Column" } }} }) {
            source {
                id
                fullName
                displayName
                stringAttributes(where: { type: { publicId: { eq: "Description" } } }) {
                    id
                    stringValue
                }
                incomingRelations(limit: 10, where: { source: {type: { publicId: { eq: "BusinessTerm" } }} }) {
                    source {
                        id
                        fullName
                        displayName
                    }
                }
            }
        }
    }
}
"""

GET_AWS_TABLE_BUSINESS_TERMS_QUERY = """
query Assets($assetId: UUID!) {
    assets(
        limit: 1
        where: {
            type: { publicId: { eq: "Table" } }
            id: { eq: $assetId }
        }
    ) {
        id
        fullName
        displayName
        incomingRelations(limit: 1000, where: { source: {type: { publicId: { eq: "BusinessTerm" } }} }) {
            source {
                id
                fullName
                displayName
            }
        }
    }
}
"""

GET_PII_COLUMNS_QUERY = """
query Assets($assetId: UUID!) {
    assets(
        limit: 1
        where: { type: { publicId: { eq: "Table" } }, id: { eq: $assetId } }
    ) {
        incomingRelations(
            limit: 1000
            where: { source: { type: { publicId: { eq: "Column" } } } }
        ) {
            source {
                displayName
                incomingRelations(
                    limit: 10
                ) {
                    source {
                        incomingRelations(
                            limit: 1
                            where: { source: { displayName: { eq: "Personal Identifiable Information" } type: { publicId: { eq: "DataCategory" }}} }
                        ) {
                            source {
                                displayName
                                type {
                                    publicId
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
"""

GET_BUSINESS_TERM_HIERARCHY_QUERY = """
query Assets {
    assets(
        limit: 600
        where: {
            type: { publicId: { eq: "BusinessTerm" } }
            incomingRelations: { empty: false }
        }
    ) {
        displayName
        incomingRelations(
            limit: 40
            where: { source: { type: { publicId: { eq: "BusinessTerm" } } } }
        ) {
            source {
                displayName
            }
        }
    }
}
"""

GET_TABLE_BY_NAME_QUERY = """
query Assets($tableName: String!) {
    assets(
        limit: 1
        where: {
            type: { publicId: { eq: "Table" } }
            displayName: { eq: $tableName }
            fullName: {
                startsWith: "AWS"
            }
        }
    ) {
        id
    }
}
"""

GET_SUBSCRIPTION_REQUESTS_BY_STATUS_QUERY = """
query Assets($status: String!) {
    assets(
        limit: 100
        where: {
            displayName: { contains: "Subscription Request" }
            status: { name: { eq: $status } }
            outgoingRelations: { empty: false }
        }
        order: { modifiedOn: desc }
    ) {
        id
        displayName
        outgoingRelations(
            limit: 1
            where: { target: { fullName: { startsWith: "AWS" } } }
        ) {
            target {
                id
                fullName
                displayName
                stringAttributes(where: { type: { name: { eq: "AWS Resource Metadata" } } }) {
                    stringValue
                    type {
                        name
                    }
                }
            }
        }
        stringAttributes(where: { type: { name: { in: ["AWS Producer Project Id", "AWS Consumer Project Id"] } } }) {
            stringValue
            type {
                name
            }
        }
    }
}
"""

GET_ASSET_BY_NAME_QUERY = """
query Assets($assetName: String!) {
    assets(
        limit: 1
        where: {
            displayName: { eq: $assetName }
        }
    ) {
        id
        fullName
        displayName
    }
}
"""

GET_ASSET_BY_NAME_AND_TYPE_QUERY = """
query Assets($assetName: String!, $typeId: UUID!) {
    assets(limit: 1, where: { displayName: { eq: $assetName } type: {id: { eq: $typeId}}}) {
        id
        fullName
        displayName
    }
}
"""


GET_ASSET_AND_STRING_ATTRIBUTES_BY_NAME_AND_TYPE_QUERY = """
query Assets($assetName: String!, $type: UUID!, $stringAttributeType: UUID!) {
    assets(
        limit: 1
        where: {
            displayName: { eq: $assetName }
            type: { id: { eq: $type } }
        }
    ) {
        id
        fullName
        displayName
        stringAttributes(where: {type: {id: {eq: $stringAttributeType}}}) {
            id
            stringValue
        }
    }
}
"""