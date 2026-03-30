openapi: 3.0.1
info:
  title: UDISE+ (Unified District Information System for Education)
  version: 1.0.0
servers:
- url: https://api.mospi.gov.in/
tags:
- name: UDISE
  description: Indicator-wise school education statistics from UDISE+ in India.
paths:
  /api/udise/getUdiseRecords:
    get:
      summary: Get data for an indicator
      description: "- **Required:** `indicator_code`  \n- **Filters (optional):**\
        \ sub_indicator_code,gender_code,state_code,category_code,social_group_code,age_group_code,class_code,enrolement_bracket_code,facilit_of_labs_code,infrastructure_facility_code,level_of_education_code,source_of_drinking_water_code,type_of_management_code,year\n"
      parameters:
      - in: query
        name: indicator_code
        required: true
        schema:
          type: integer
          pattern: ^\d+$
        description: Indicator code (primary filter, single value only)
      - in: query
        name: year
        schema:
          type: string
          pattern: ^\d{4}-\d{2}$
        description: Academic year (e.g., 2024-25). Range is 2018-19 to 2024-25.
      - in: query
        name: state_code
        schema:
          type: integer
        description: State code (1-38). Use get_metadata to get valid codes per indicator.
      - in: query
        name: gender_code
        schema:
          type: integer
        description: "Gender code: 1=Boys/Male, 2=Girls/Female, 3=Total"
      - in: query
        name: type_of_management_code
        schema:
          type: integer
        description: "Management type: 1=All Management, 2=Government, 3=Government Aided, 4=Private Unaided Recognized, 5=Others"
      - in: query
        name: level_of_education_code
        schema:
          type: integer
        description: Level of education code (Pre-Primary, Primary 1-5, Upper Primary 6-8, Secondary 9-10, Higher Secondary 11-12, etc.). Use get_metadata for valid codes per indicator.
      - in: query
        name: sub_indicator_code
        schema:
          type: integer
        description: Sub-indicator code. Required for some indicators (e.g., 45, 47, 48, 49). Use get_metadata for valid values.
      - in: query
        name: age_group_code
        schema:
          type: integer
        description: "Age group code (indicator 34 - ASER): 1=6-10yrs, 2=11-13yrs, 3=14-15yrs, 4=16-17yrs, 5=6-13yrs"
      - in: query
        name: class_code
        schema:
          type: integer
        description: Class code (indicator 24 - teachers by class taught). Use get_metadata for valid values.
      - in: query
        name: enrolement_bracket_code
        schema:
          type: integer
        description: Enrolment bracket code (indicator 14 - schools by enrolment bracket). Use get_metadata for valid values.
      - in: query
        name: infrastructure_facility_code
        schema:
          type: integer
        description: Infrastructure facility code (indicator 16 - schools by infrastructure facility). Use get_metadata for valid values.
      - in: query
        name: source_of_drinking_water_code
        schema:
          type: integer
        description: "Source of drinking water code (indicator 45): 1=Any source, 2=Tap Water, 3=Packed Water, 4=Hand Pump, 5=Well, 6=Unprotected Well, 7=Other"
      - in: query
        name: facilit_of_labs_code
        schema:
          type: integer
        description: "Facility of labs code (indicator 49 - ICT Labs): 1=Total, 2=ICT labs, 3=Functional ICT Labs"
      - in: query
        name: category_code
        schema:
          type: integer
        description: Category code (if applicable for the indicator).
      - in: query
        name: social_group_code
        schema:
          type: integer
        description: Social group code (if applicable for the indicator).
      - in: query
        name: limit
        schema:
          type: integer
          default: 20
          pattern: ^\d+$
      - in: query
        name: page
        schema:
          type: integer
          minimum: 1
          pattern: ^\d+$
      responses:
        '200':
          description: A JSON array of UDISE records
        '400':
          description: Bad Request
          content:
            application/json:
              schema:
                type: object
                properties:
                  statusCode:
                    type: boolean
                    example: false
                  message:
                    type: string
                    example: Invalid request body
      security:
      - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: apiKey
      description: Bearer token to access these api endpoints
      name: Authorization
      in: header
x-original-swagger-version: '2.0'
