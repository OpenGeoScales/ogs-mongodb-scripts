db.createView(
    "data_cartography",
    "emissions",
    [{$lookup: {
      from: 'geo_components',
      localField: 'geo_component_id',
      foreignField: '_id',
      as: 'geo_component'
    }}, {$lookup: {
      from: 'data_sources',
      localField: 'data_source_id',
      foreignField: '_id',
      as: 'data_source'
    }}, {$project: {
      scale: {$first: '$geo_component.scale'},
      data_source: {$first: '$data_source.name'},
      date: '$date',
      gas: '$gas',
      sector: '$sector.sector_mapped_name'
    }}, {$group: {
      _id: {
        'scale': '$scale',
        'data_source': '$data_source'
      },
      dates: {
        $addToSet: "$date"
      },
      sectors: {
        $addToSet: '$sector'
      },
      gas: {
        $addToSet: '$gas'
      }
    }}, {$group: {
      _id: "$_id.scale",
      data_sources: {
        $push: {
            name: "$_id.data_source",
            dates: "$dates",
            sectors: "$sectors",
            gas: "$gas"
        }
      }
    }}]
)