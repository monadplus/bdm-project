# Datasets

## Idealista

Attributes:

- propertyCode: id
- thumbnail
- externalReference
- numPhotos
- floor
- price
- propertyType
- operation: sale/rent
- size
- exterior
- rooms
- bathrooms
- address
- province
- municipality
- district
- country
- neighborhood
- latitude
- longitude
- showAddress
- url
- distance: to the city center
- hasVideo
- status
- newDevelopment
- hasLift
- priceByArea
- detailedType: object
- suggestedTexts: object
- hasPlan
- has3DTour
- has360
- hasStaging
- topNewDevelopment

| Idealista    | lookup table |
|--------------|--------------|
| district     | district     |
| neighborhood | neighborhood |

## OpenData

- Any
- Codi_Districte
- Nom_Districte
- Codi_Barri
- Nom_Barri
- Població
- Índex RFD Barcelona = 100

| Opendata       | lookup table |
|----------------|--------------|
| Codi_Districte | district     |
| Nom_Barri      | neighborhood |
