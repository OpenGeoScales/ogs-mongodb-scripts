db.createView(
    "country_emissions",
    "emissions",
    [{$lookup: {
      from: 'data_sources',
      localField: 'data_source_id',
      foreignField: '_id',
      as: 'data_source'
    }}, {$project: {
        geo_component_id: "$geo_component_id",
        date: "$date",
        gas: "$gas",
        data_source: {$first: '$data_source.name'},
        unit: "$unit.unit_used",
            sector: "$sector.sector_mapped_name",
        emissions: '$emissions',
        value: "$value",
    }}, {$group: {
      _id: "$geo_component_id",
      emissions: {
       $push: {
         date: "$date",
         gas: "$gas",
         value: "$value",
         unit: "$unit",
         data_source: "$data_source",
         sector: "$sector"
       }
      }
    }}, {$lookup: {
      from: 'geo_components',
      localField: '_id',
      foreignField: '_id',
      as: 'geo_component'
    }}, {$project: {
        geo_component: {
          $first: '$geo_component'
        },
        emissions: '$emissions'
    }}]
)