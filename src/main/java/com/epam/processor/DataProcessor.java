package com.epam.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.epam.data.RoadAccident;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This is to be completed by mentees
 */
public class DataProcessor {

    private final List<RoadAccident> roadAccidentList;

    public DataProcessor(List<RoadAccident> roadAccidentList){
        this.roadAccidentList = roadAccidentList;
    }


//    First try to solve task using java 7 style for processing collections

    /**
     * Return road accident with matching index
     * @param index
     * @return
     */
    public RoadAccident getAccidentByIndex7(String index){
    	for (RoadAccident roadAccident : roadAccidentList) {
    		if (roadAccident.getAccidentId().equals(index)) {
    			return roadAccident;
    		}
    	}
        return null;
    }


    /**
     * filter list by longtitude and latitude values, including boundaries
     * @param minLongitude
     * @param maxLongitude
     * @param minLatitude
     * @param maxLatitude
     * @return
     */
    public Collection<RoadAccident> getAccidentsByLocation7(float minLongitude, float maxLongitude, float minLatitude, float maxLatitude){
    	Collection<RoadAccident> results = roadAccidentList.stream().filter(item -> 
			(item.getLongitude() >= minLongitude && item.getLongitude() <= maxLongitude) 
			&&
			(item.getLatitude() >= minLatitude && item.getLatitude() <= maxLatitude) 
			).collect(Collectors.toList());
    	return results;
    }

    /**
     * count incidents by road surface conditions
     * ex:
     * wet -> 2
     * dry -> 5
     * @return
     */
    public Map<String, Long> getCountByRoadSurfaceCondition7(){
    	Map<String, Long> results = new HashMap<>();
    	roadAccidentList.stream().forEach(item -> {
    		String condition = item.getRoadSurfaceConditions();
    		if (null == results.get(condition)) {
    			results.put(condition, 1L);
    		} else {
    			results.put(condition, results.get(condition) + 1);
    		}
    	});
        return results;
    }

    /**
     * find the weather conditions which caused the top 3 number of incidents
     * as example if there were 10 accidence in rain, 5 in snow, 6 in sunny and 1 in foggy, then your result list should contain {rain, sunny, snow} - top three in decreasing order
     * @return
     */
    public List<String> getTopThreeWeatherCondition7(){
    	Map<String, List<RoadAccident>> analysis = roadAccidentList.stream().collect(Collectors.groupingBy(item -> item.getWeatherConditions()));
    	return analysis.values().stream()
    		.sorted((pre, next) -> next.size() - pre.size())
    		.limit(3)
    		.map(item -> item.get(0).getWeatherConditions())
    		.collect(Collectors.toList());
    }

    /**
     * return a multimap where key is a district authority and values are accident ids
     * ex:
     * authority1 -> id1, id2, id3
     * authority2 -> id4, id5
     * @return
     */
    public Multimap<String, String> getAccidentIdsGroupedByAuthority7(){
    	Multimap<String, String> results = ArrayListMultimap.create();
    	for (RoadAccident roadAccident : roadAccidentList) {
    		Collection<String> items = results.get(roadAccident.getDistrictAuthority());
    		items.add(roadAccident.getAccidentId());
    	}
        return results;
    }


    // Now let's do same tasks but now with streaming api



    public RoadAccident getAccidentByIndex(String index){
    	Optional<RoadAccident> result = roadAccidentList.stream()
    			.filter(item -> item.getAccidentId().equals(index))
    			.findFirst();
    	if (result.isPresent()) {
    		return result.get();
    	}
        return null;
    }


    /**
     * filter list by longtitude and latitude fields
     * @param minLongitude
     * @param maxLongitude
     * @param minLatitude
     * @param maxLatitude
     * @return
     */
    public Collection<RoadAccident> getAccidentsByLocation(float minLongitude, float maxLongitude, float minLatitude, float maxLatitude){
    	Collection<RoadAccident> results = roadAccidentList.stream().filter(item -> 
    		(item.getLongitude() >= minLongitude && item.getLongitude() <= maxLongitude) 
    		&&
    		(item.getLatitude() >= minLatitude && item.getLatitude() <= maxLatitude) 
    		).collect(Collectors.toList());
        return results;
    }

    /**
     * find the weather conditions which caused max number of incidents
     * @return
     */
    public List<String> getTopThreeWeatherCondition(){
    	Map<String, List<RoadAccident>> analysis = roadAccidentList.stream().collect(Collectors.groupingBy(item -> item.getWeatherConditions()));
    	return analysis.values().stream()
    		.sorted((pre, next) -> next.size() - pre.size())
    		.limit(3)
    		.map(item -> item.get(0).getWeatherConditions())
    		.collect(Collectors.toList());
    }

    /**
     * count incidents by road surface conditions
     * @return
     */
    public Map<String, Long> getCountByRoadSurfaceCondition(){
    	Map<String, Long> results = new HashMap<>();
    	roadAccidentList.stream().forEach(item -> {
    		String condition = item.getRoadSurfaceConditions();
    		if (null == results.get(condition)) {
    			results.put(condition, 1L);
    		} else {
    			results.put(condition, results.get(condition) + 1);
    		}
    	});
        return results;
    }

    /**
     * To match streaming operations result, return type is a java collection instead of multimap
     * @return
     */
    public Map<String, List<String>> getAccidentIdsGroupedByAuthority(){
    	Map<String, List<String>> results = new HashMap<>();
    	roadAccidentList.stream().forEach(item -> {
    		String authority = item.getDistrictAuthority();
    		List<String> ids = results.get(authority);
    		if (null == ids) {
    			ids = new ArrayList<>();
    			ids.add(item.getAccidentId());
    			results.put(authority, ids);
    		} else {
    			ids.add(item.getAccidentId());
    		}
    	});
        return results;
    }

}
