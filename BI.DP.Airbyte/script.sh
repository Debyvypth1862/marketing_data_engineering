#!/bin/bash


# Fetch all source definitions from Airbyte for the specified workspace
response=$(curl -s -X POST "$AIRBYTE_API_URL/source_definitions/list_for_workspace" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId": "'$WORKSPACE_ID'"}')

json_data=$(jq -r '.sourceDefinitions[]' <<< "$response")

source_definition=$(jq -r '. | select(.name == "'"$SOURCE_NAME"'")' <<< "$json_data")
echo "Source definition for \"$SOURCE_NAME\": $source_definition"

# Check if the source definition was found
if [[ -z "$source_definition" ]]; then
  echo "Service \"$SOURCE_NAME\" not found in the provided data."
  curl -s -X POST "$AIRBYTE_API_URL/source_definitions/create_custom" \
    -H "Content-Type: application/json" \
    -d '{
        "workspaceId": "'$WORKSPACE_ID'",
        "sourceDefinition": {
          "name": "'$SOURCE_NAME'",
          "documentationUrl": "",
          "dockerImageTag": "'$IMAGE_TAG'",
          "dockerRepository": "'$REGISTRY/$REPOSITORY'"
        }
    }'
else
  # Extract the source ID from the found source definition
  source_id=$(echo "$source_definition" | jq -r '.sourceDefinitionId')
  echo "Source ID for \"$SOURCE_NAME\": $source_id"
  curl -s -X POST "$AIRBYTE_API_URL/source_definitions/update" \
    -H "Content-Type: application/json" \
    -d '{
      "sourceDefinitionId": "'$source_id'",
      "dockerImageTag": "'$IMAGE_TAG'"
    }'
fi