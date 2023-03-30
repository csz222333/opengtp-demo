package com.combest.report.service.impl;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import com.combest.production.bean.MixtureDailyReport;
import com.combest.production.bean.MixtureRecord;
import com.combest.production.bean.OneMixedTemperatureRecord;
import com.combest.production.bean.ProcedureOrder;
import com.combest.production.bean.ProductionOrderDO;
import com.combest.production.bean.RawMaterial;
import com.combest.production.dto.ParameterSubDTO;
import com.combest.production.dto.TrafficPressureSubDTO;
import com.combest.production.mapper.ProductionOrderMapper;
import com.combest.production.mapper.TrafficPressureSubMapper;
import com.combest.production.service.ParameterSubService;
import com.combest.production.service.RawMaterialService;
import com.combest.report.bean.FireCheckStatistics;
import com.combest.report.bean.FireCheckStatistics.BatchingOne;
import com.combest.report.bean.FireCheckStatistics.BatchingTwo;
import com.combest.report.bean.FireCheckStatistics.Feeding;
import com.combest.report.bean.FireCheckStatistics.InstallBowl;
import com.combest.report.bean.FireCheckStatistics.MixedBatch;
import com.combest.report.bean.FireCheckStatistics.SinterOne;
import com.combest.report.bean.FireCheckStatistics.SinterTwo;
import com.combest.report.bean.FireCheckStatistics.Smash;
import com.combest.report.bean.FireCheckStatistics.Washing;
import com.combest.report.bean.qo.CheckStatisticsQO;
import com.combest.report.bean.vo.ProcedureVO;
import com.combest.report.bean.vo.ProcedureSumVO;
import com.combest.report.mapper.FireCheckStatisticsMapper;
import com.combest.report.service.FireCheckStatisticsService;
import com.combest.ssny_common.util.ProcedureOrderTypeEnum;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FireCheckStatisticsServiceImpl implements FireCheckStatisticsService {

	@Autowired
	private ProductionOrderMapper productionOrderMapper;

	@Autowired
	private FireCheckStatisticsMapper fireCheckStatisticsMapper;

	@Autowired
	RawMaterialService rawMaterialService;
	
	@Autowired
	private TrafficPressureSubMapper trafficPressureSubMapper;
	
	@Autowired
    private ParameterSubService parameterSubService;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	@Override
	public Map<String, Object> getData(CheckStatisticsQO qo) {

		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("msg", "");
		dataMap.put("code", 0);
		if (qo == null) {
			qo = new CheckStatisticsQO();
		}

		// 查询时间范围内的指令单

		return dataMap;
	}

	@Override
	public LinkedHashMap<String, FireCheckStatistics> importData(CheckStatisticsQO qo) {
		// 第一步查询工序工单
		List<ProcedureVO> listPageByWorkshopFireCheck = fireCheckStatisticsMapper.listPageByWorkshopFireCheck(qo);
		// 指令单的个数
		int orderIdCount =0;
		
		// 计算orderId字段的数量
		// 然后根据指令单id取工序工单
				LinkedHashMap<String, ArrayList<ProcedureVO>> orderMap = new LinkedHashMap<>();
				LinkedHashMap<String, FireCheckStatistics> resultMap = new LinkedHashMap<>();
				// 根据工序类型进行分类,以及通过根据车间Id创建FireCheckStatistics结果集
		try {
			orderIdCount= (int) listPageByWorkshopFireCheck.stream()
				    .map(ProcedureVO::getOrderId) // 获取 orderId 属性
				    .filter(Objects::nonNull) // 过滤掉为 null 的值
				    .distinct() // 去除重复值
				    .count(); // 计算个数

		}catch (Exception e) {
			System.out.println("报错>条数"+listPageByWorkshopFireCheck.size());
			e.printStackTrace();
			return resultMap;
		}
		
		for (ProcedureVO item : listPageByWorkshopFireCheck) {

			ArrayList<ProcedureVO> list = new ArrayList<ProcedureVO>();
			if (orderMap.containsKey(item.getType())) {
				list = orderMap.get(item.getType());
			}
			String key = item.getWorkshopId() + "-workshopId";
			if (!resultMap.containsKey(key)) {
				FireCheckStatistics data = new FireCheckStatistics();
				data.setProductionLineId(item.getProductionLineId());
				data.setProductionLineName(item.getProductionLine());
				data.setWorkshopId(item.getWorkshopId());
				data.setWorkshopName(item.getWorkshopName());
				resultMap.put(key, data);
			}

			list.add(item);
			orderMap.put(item.getType()+"-"+item.getSerialNumber(), list);
		}

		// FireFeedingInfo 投料工序
		dealWithFeedingInfo(orderMap.get(ProcedureOrderTypeEnum.FEEDING.getType()+"-1"), resultMap);
		// BatchingInfo 火法一配
		dealWithBatchingInfo(orderMap.get(ProcedureOrderTypeEnum.BATCHING.getType()+"-1"), resultMap,qo);
		// InstallBowl 火法装钵
		dealWithInstallBowl(orderMap.get(ProcedureOrderTypeEnum.INSTALL_BOWL.getType()+"-1"), resultMap,qo);
		// Sinter 火法一次烧结
		dealWithSinter(orderMap.get(ProcedureOrderTypeEnum.SINTER.getType()+"-1"), resultMap,qo);
		//Smash 粉碎工序
		dealWithSmash(orderMap.get(ProcedureOrderTypeEnum.SMASH.getType()+"-1"), resultMap,qo);
		//Washing 水洗干燥工序
		dealWithWashing(orderMap.get(ProcedureOrderTypeEnum.WASHING.getType()+"-1"), resultMap,qo);
		//二次配料工序 BatchingTwo
		dealWithBatchingTwo(orderMap.get(ProcedureOrderTypeEnum.BATCHING.getType()+"-2"), resultMap,qo);
		// SinterTwo 火法二次烧结
		dealWithSinterTwo(orderMap.get(ProcedureOrderTypeEnum.SINTER.getType()+"-2"), resultMap,qo);
		//  混批  MixedBatch
		dealWithMixedBatch(orderMap.get(ProcedureOrderTypeEnum.MIXED_BATCH.getType()+"-1"), resultMap,qo);
		
		return resultMap;

	}

	// ============计算投料工序
	public void dealWithFeedingInfo(List<ProcedureVO> orderList, Map<String, FireCheckStatistics> resultMap) {
		//可以对该条SQL进行优化，不要一次性查所有的
	    List<RawMaterial> rawMaterialList = rawMaterialService.listByRawMaterial(new RawMaterial(), null, null);
	    Map<Integer, List<RawMaterial>> rawMaterialMap = rawMaterialList.stream().collect(Collectors.groupingBy(RawMaterial::getProcedureOrderId));

	    if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}

	    resultMap.values().stream()
	        .forEach(statistics -> {
	            orderList.stream()
	                .filter(procedure -> procedure.getWorkshopId() == statistics.getWorkshopId())
	                .forEach(procedure -> {
	                    Feeding feeding = new FireCheckStatistics().new Feeding();
	                    feeding.setWorkshopName(procedure.getWorkshopName());
	                    feeding.setProductionLineName(procedure.getProductionLine());
	                    List<Feeding> feedingList = new ArrayList<>();
	                    feeding.setWeighNum(0);
	                    int index = 0;
	                    if (CollectionUtils.isEmpty(statistics.getFeedList())) {
	                        feeding.setPlanNum(1);
	                        if (procedure.getProductionScheduleStatus()!=null&&procedure.getProductionScheduleStatus() == 31) {
	                            feeding.setNum(1);
	                        }
	                        feedingList.add(feeding);
	                    } else {
	                        feedingList = statistics.getFeedList();
	                        for (Feeding item : feedingList) {
	                            index++;
	                            if (Objects.equals(item.getProductionLineName(), procedure.getProductionLine())) {
	                                feeding = item;
	                                feeding.setPlanNum(feeding.getPlanNum() + 1);
	                                if (procedure.getProductionScheduleStatus()!=null&&procedure.getProductionScheduleStatus() == 31) {
	                                    feeding.setNum(feeding.getNum() + 1);
	                                }
	                            }
	                        }
	                    }
	                    List<RawMaterial> rawMaterial = rawMaterialMap.get(procedure.getId());
	                    if (CollectionUtils.isNotEmpty(rawMaterial)) {
	                        feeding.setWeighNum(feeding.getWeighNum() + rawMaterial.size());
	                    }
	                    feedingList.set(index, feeding);
	                    statistics.setFeedList(feedingList);
	                });
	        });
	}


	// ============计算一配工单
	public void dealWithBatchingInfo(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo){
		//查询所有的混料明细
		CheckStatisticsQO qos = new CheckStatisticsQO();
		qos =qo;
		qos.setTableName("mixture_record");
		if(orderList==null||resultMap.values()==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}


		List<ProcedureSumVO> mixList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		HashMap<Integer, ProcedureSumVO> hashMap = new HashMap<>();
		
		for(ProcedureSumVO vo :mixList) {
			hashMap.put(vo.getId(), vo);
		}
		
		
		
		//查询所有的混料日报
		List<MixtureDailyReport> selectMixtureDailyReportByProcedure = fireCheckStatisticsMapper.selectMixtureDailyReportByProcedure(qo);
		Map<Integer, List<MixtureDailyReport>> mixtureDailyReportMap = selectMixtureDailyReportByProcedure.stream().collect(Collectors.groupingBy(MixtureDailyReport::getProcedureOrderId));
		//查询一配温度记录计划
		List<OneMixedTemperatureRecord> selectOneMixedTemperatureRecordByProcedure = fireCheckStatisticsMapper.selectOneMixedTemperatureRecordByProcedure(qo);
		Map<Integer, List<OneMixedTemperatureRecord>> oneMixedTemperatureRecordMap = selectOneMixedTemperatureRecordByProcedure.stream().collect(Collectors.groupingBy(OneMixedTemperatureRecord::getProcedureOrderId));
		resultMap.values().stream().forEach( item ->{
			orderList.stream()
			.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
			.forEach(procedure->{
				BatchingOne batchingOne = new FireCheckStatistics().new BatchingOne();
				batchingOne.setWorkshopName(procedure.getWorkshopName());
				batchingOne.setProductionLineName(procedure.getProductionLine());
				batchingOne.setMixedDayNum(0);
				batchingOne.setMixedDayPlanNum(0);
				batchingOne.setMixedNum(0);
				batchingOne.setMixedPlanNum(0);
				batchingOne.setTemperatureNum(0);
				batchingOne.setTemperaturePlanNum(0);
				
				List<BatchingOne> batchingOneList = new ArrayList<>();
				
				int index = 0;
				
				if(!CollectionUtils.isEmpty(item.getBatchingOneList())) {
					batchingOneList = item.getBatchingOneList();
					for (BatchingOne sub : batchingOneList) {
						index++;
						if(Objects.equals(sub.getProductionLineName(), batchingOne.getProductionLineName())) {
							batchingOne = sub;
						}
					}
				}else {
					batchingOneList.add(batchingOne);
				}
				int mixtureRecordSize = ObjectUtils.isEmpty(hashMap.get(procedure.getId()))?0:hashMap.get(procedure.getId()).getProcedureSize();
				int mixtureDailyReportSize = ObjectUtils.isEmpty(mixtureDailyReportMap.get(procedure.getId()))?0:mixtureDailyReportMap.get(procedure.getId()).size();
				int oneMixedTemperatureRecordSize = ObjectUtils.isEmpty(oneMixedTemperatureRecordMap.get(procedure.getId()))?0:oneMixedTemperatureRecordMap.get(procedure.getId()).size();
				if(procedure.getProductionScheduleStatus()!=null&&procedure.getProductionScheduleStatus() == 41) {
					//混料明细
					batchingOne.setMixedNum(mixtureRecordSize+batchingOne.getMixedNum());
					//配料日报
					batchingOne.setMixedDayNum(mixtureDailyReportSize+batchingOne.getMixedDayNum());
					List<OneMixedTemperatureRecord> list = oneMixedTemperatureRecordMap.get(procedure.getId());
					Long size = list.stream().filter(temp -> !ObjectUtils.isArray(temp.getBeforeRoedigerLeft())).count();
					
					//温度计划
					batchingOne.setTemperatureNum(size.intValue()+oneMixedTemperatureRecordSize);
				}
				batchingOne.setMixedPlanNum(mixtureRecordSize+batchingOne.getMixedNum());
				//配料日报
				batchingOne.setMixedDayPlanNum(mixtureDailyReportSize+batchingOne.getMixedDayNum());
				//温度计划
				
				
				batchingOne.setTemperaturePlanNum(batchingOne.getTemperatureNum()+oneMixedTemperatureRecordSize);
				batchingOneList.set(index, batchingOne);
				item.setBatchingOneList(batchingOneList);
			});
		});
	}
	
	// ============计算火法装钵
	public void dealWithInstallBowl(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo) {

		qo.setTableName("bowl_run_record");
		if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}

		
		List<ProcedureSumVO> selectTableNameByProcedure = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
	
		Map<Integer, ProcedureSumVO> map = new HashMap<>();
		for (ProcedureSumVO procedureSumVO : selectTableNameByProcedure) {
		    map.put(procedureSumVO.getId(), procedureSumVO);
		}

		
		resultMap.values().stream().forEach( item ->{
			orderList.stream()
			.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
			.forEach(procedure->{
				InstallBowl installBowl = new FireCheckStatistics().new InstallBowl();
				installBowl.setWorkshopName(procedure.getWorkshopName());
				installBowl.setProductionLineName(procedure.getProductionLine());
				List<InstallBowl> installBowlList = new ArrayList<>();
				installBowl.setNum(0);
				installBowl.setPlanNum(0);
				
				
				int index = 0;
				
				if(!CollectionUtils.isEmpty(item.getInstallBowlList())){
					installBowlList = item.getInstallBowlList();
					for (InstallBowl bowl : installBowlList) {
						index++;
						if(bowl.getProductionLineName().equals(installBowl.getProductionLineName())) {
							installBowl = bowl;
						}
					}
				}else {
					installBowlList.add(installBowl);
				}
				
				LocalDate startDate =qo.getStartTime();
				LocalDate endDate = qo.getEndTime();
				Duration duration = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay());
				long days = duration.toDays(); // 计算天数

				int size = ObjectUtils.isEmpty(map.get(procedure.getId()))?0:map.get(procedure.getId()).getProcedureSize();
				installBowl.setPlanNum(installBowl.getPlanNum()+size*2);
				installBowl.setNum(installBowl.getNum()+size);
				installBowlList.set(index, installBowl);
				item.setInstallBowlList(installBowlList);
				
				
			});
		});
	}
	
	//============ 水洗干燥工序
	public void dealWithWashing(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo) {
		//干燥
		qo.setTableName("washing_order");
		List<ProcedureSumVO> selectTableNameByProcedure = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
		Map<Integer, ProcedureSumVO> map = new HashMap<>();
		for (ProcedureSumVO procedureSumVO : selectTableNameByProcedure) {
		    map.put(procedureSumVO.getId(), procedureSumVO);
		}
		
		if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}


		resultMap.values().stream().forEach( item ->{
			orderList.stream()
			.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
			.forEach(procedure->{
				Washing washing = new FireCheckStatistics().new Washing();
				washing.setWorkshopName(procedure.getWorkshopName());
				washing.setProductionLineName(procedure.getProductionLine());
				washing.setNum(0);
				washing.setPlanNum(0);
				
				List<Washing> washingTwoList = new ArrayList<>();
				
				int index = 0;
				
				if(!CollectionUtils.isEmpty(item.getWashingList())){
					washingTwoList = item.getWashingList();
					for (Washing bowl : washingTwoList) {
						index++;
						if(bowl.getProductionLineName().equals(washing.getProductionLineName())) {
							washing = bowl;
						}
					}
				}else {
					washingTwoList.add(washing);
				}
				
				Integer procedureSize = ObjectUtils.isEmpty(map.get(procedure.getId()).getProcedureSize())?0:map.get(procedure.getId()).getProcedureSize();
				
				if(procedure.getProductionScheduleStatus()!=null&&procedure.getProductionScheduleStatus()==51) {
					washing.setNum(washing.getNum()+procedureSize);
				}
				
				washing.setPlanNum(procedureSize+washing.getPlanNum());
				
				washingTwoList.set(index, washing);
				item.setWashingList(washingTwoList);
				
			});
		});
	}

	// ============计算粉碎工序
	public void dealWithSmash(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo) {
		if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}

			//除磁
			qo.setTableName("remove_magnet_record");
			List<ProcedureSumVO> selectTableNameByProcedure = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
			//筛上物记录 oversize_product
			qo.setTableName("oversize_product");
			List<ProcedureSumVO> orversizeList = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
			//粉碎产出记录 produce_record
			qo.setTableName("produce_record");
			List<ProcedureSumVO> produceList = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
			//电磁除铁参数记录表 
			qo.setTableName("electromagnetic_remove_iron_parameter_record");
			List<ProcedureSumVO> electList = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
			
			
			Map<Integer, ProcedureSumVO> map = new HashMap<>();
			Map<Integer, ProcedureSumVO> oversizemap = new HashMap<>();
			Map<Integer, ProcedureSumVO> producemap = new HashMap<>();
			Map<Integer, ProcedureSumVO> electmap = new HashMap<>();
			
			
			for (ProcedureSumVO procedureSumVO : selectTableNameByProcedure) {
			    map.put(procedureSumVO.getId(), procedureSumVO);
			}
			
			for (ProcedureSumVO procedureSumVO : orversizeList) {
				oversizemap.put(procedureSumVO.getId(), procedureSumVO);
			}
			
			for (ProcedureSumVO procedureSumVO : electList) {
				producemap.put(procedureSumVO.getId(), procedureSumVO);
			}
			
			for (ProcedureSumVO procedureSumVO : selectTableNameByProcedure) {
				electmap.put(procedureSumVO.getId(), procedureSumVO);
			}
			
			
			


			resultMap.values().stream().forEach( item ->{
				orderList.stream()
				.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
				.forEach(procedure->{
					Smash smash = new FireCheckStatistics().new Smash();
					smash.setWorkshopName(procedure.getWorkshopName());
					smash.setProductionLineName(procedure.getProductionLine());
					smash.setEraseNum(0);
					smash.setErasePlanNum(0);
					smash.setMagnetNum(0);
					smash.setMagnetPlanNum(0);
					smash.setNum(0);
					smash.setPlanNum(0);
					smash.setOvertailsNum(0);
					smash.setOvertailsPlanNum(0);
					
					
					List<Smash> smashList = new ArrayList<>();
					
					int index = 0;
					
					if(!CollectionUtils.isEmpty(item.getSmashList())){
						smashList = item.getSmashList();
						for (Smash bowl : smashList) {
							index++;
							if(bowl.getProductionLineName().equals(smash.getProductionLineName())) {
								smash = bowl;
							}
						}
					}else {
						smashList.add(smash);
					}
					
					Integer procedureSize = ObjectUtils.isEmpty(map.get(procedure.getId()))?0:map.get(procedure.getId()).getProcedureSize();
					
					LocalDate startDate =qo.getStartTime();
					LocalDate endDate = qo.getEndTime();
					Duration duration = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay());
					Long days = duration.toDays(); // 计算天数
					
					
					Integer removeSize = ObjectUtils.isEmpty(map.get(procedure.getId()))?0:map.get(procedure.getId()).getProcedureSize();
					Integer overSiz = ObjectUtils.isEmpty(oversizemap.get(procedure.getId()))?0:oversizemap.get(procedure.getId()).getProcedureSize();
					Integer produceSize = ObjectUtils.isEmpty(producemap.get(procedure.getId()))?0:producemap.get(procedure.getId()).getProcedureSize();
					Integer electSize = ObjectUtils.isEmpty(electmap.get(procedure.getId()))?0:electmap.get(procedure.getId()).getProcedureSize();
					
					
					if(procedure.getProductionScheduleStatus()!=null&&procedure.getProductionScheduleStatus()==49) {
						smash.setNum(smash.getNum()+produceSize);
					}
					smash.setPlanNum(produceSize+smash.getPlanNum());
					
					smash.setErasePlanNum(days.intValue()*2+smash.getErasePlanNum());
					smash.setEraseNum(removeSize+smash.getEraseNum());
					
					smash.setOvertailsPlanNum(days.intValue()*2+smash.getOvertailsPlanNum());
					smash.setOvertailsNum(overSiz+smash.getOvertailsNum());
					
					smash.setMagnetPlanNum(days.intValue()*2+smash.getMagnetPlanNum());
					smash.setMagnetNum(electSize+smash.getMagnetNum());
										
					smashList.set(index, smash);
					item.setSmashList(smashList);
					
				});
			});
		}
	
	// ============计算二次配料工序
	public void dealWithBatchingTwo(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo) {
		if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}

		//混料明细
		qo.setTableName("mixture_record");
		List<ProcedureSumVO> selectTableNameByProcedure = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
		//配料日报
		qo.setTableName("mixture_daily_report");
		List<ProcedureSumVO> reportList = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
		
		Map<Integer, ProcedureSumVO> map = new HashMap<>();
		for (ProcedureSumVO procedureSumVO : selectTableNameByProcedure) {
		    map.put(procedureSumVO.getId(), procedureSumVO);
		}
		
		Map<Integer, ProcedureSumVO> reportMap = new HashMap<>();
		for (ProcedureSumVO procedureSumVO : reportList) {
			reportMap.put(procedureSumVO.getId(), procedureSumVO);
		}

		resultMap.values().stream().forEach( item ->{
			orderList.stream()
			.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
			.forEach(procedure->{
				BatchingTwo batchingTwo = new FireCheckStatistics().new BatchingTwo();
				batchingTwo.setWorkshopName(procedure.getWorkshopName());
				batchingTwo.setProductionLineName(procedure.getProductionLine());
				batchingTwo.setMixedDayNum(0);
				batchingTwo.setMixedDayPlanNum(0);
				batchingTwo.setMixedNum(0);
				batchingTwo.setMixedPlanNum(0);
				
	
				List<BatchingTwo> batchingTwoList = new ArrayList<>();
				
				int index = 0;
				
				if(!CollectionUtils.isEmpty(item.getBatchingTwo())){
					batchingTwoList = item.getBatchingTwo();
					for (BatchingTwo bowl : batchingTwoList) {
						index++;
						if(bowl.getProductionLineName().equals(batchingTwo.getProductionLineName())) {
							batchingTwo = bowl;
						}
					}
				}else {
					batchingTwoList.add(batchingTwo);
				}
				
			
				
				Integer recordSize = ObjectUtils.isEmpty(map.get(procedure.getId()))?0:map.get(procedure.getId()).getProcedureSize();
				
				int reportNum = ObjectUtils.isEmpty(reportMap.get(procedure.getId()))?0:reportMap.get(procedure.getId()).getProcedureSize();
				
				if(procedure.getProductionScheduleStatus()!=null&&procedure.getProductionScheduleStatus()==46) {
					batchingTwo.setMixedDayNum(batchingTwo.getMixedDayNum()+recordSize);
					batchingTwo.setMixedNum(recordSize+batchingTwo.getMixedNum());
				}
				
				batchingTwo.setMixedDayPlanNum(recordSize+batchingTwo.getMixedDayPlanNum());
				batchingTwo.setMixedPlanNum(recordSize+batchingTwo.getMixedPlanNum());
				
				batchingTwoList.set(index, batchingTwo);
				item.setBatchingTwo(batchingTwoList);
				
			});
		});
	}
	
	// ============计算火法一次烧结
	public void dealWithSinter(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo) {
		if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}

		LocalDate startDate =qo.getStartTime();
		LocalDate endDate = qo.getEndTime();
		Duration duration = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay());
		Long days = duration.toDays(); // 计算天数
		
		CheckStatisticsQO qos = new CheckStatisticsQO();
		qos= qo;
		qos.setProduceNumber(1);
		qos.setTableName("kiln_material_in_sub");
		List<ProcedureSumVO> selectSinerNumber = fireCheckStatisticsMapper.selectSinerNumber(qos);
		
		qos.setTableName("kiln_material_out_sub");
		List<ProcedureSumVO> outList = fireCheckStatisticsMapper.selectSinerNumber(qos);
		
		qos.setTableName("produce_record");
		qos.setDynamicSQL("and category = '进窑'");
		List<ProcedureSumVO> inputList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		qos.setDynamicSQL("and category = '出窑'");
		List<ProcedureSumVO> outputList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		qos.setTableName("scrap_record");
		qos.setDynamicSQL(null);
		List<ProcedureSumVO> scrapList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		qos.setTableName("CSM_parameter");
		List<ProcedureSumVO> csmList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		qos.setTableName("ACM_parameter");
		List<ProcedureSumVO> acmList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		
		HashMap<Integer, ProcedureSumVO> inMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> outMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> outputMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> inputMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> scparpMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> csmMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> acmMap = new HashMap<>();
		
		for (ProcedureSumVO vo : selectSinerNumber) {
			inMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : outList) {
			outMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : outputList) {
			outputMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : inputList) {
			inputMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : scrapList) {
			scparpMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : scrapList) {
			csmMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : acmList) {
			acmMap.put(vo.getProductionLineId(), vo);
		}
		
		resultMap.values().stream().forEach( item ->{
			orderList.stream()
			.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
			.forEach(procedure->{
				SinterOne sinterOne = new FireCheckStatistics().new SinterOne();
				sinterOne.setWorkshopName(procedure.getWorkshopName());
				sinterOne.setProductionLineName(procedure.getProductionLine());
				sinterOne.setAcmNum(0);
				sinterOne.setCsmNum(0);
				sinterOne.setExhaustNum(0);
				sinterOne.setFlowNum(0);
				sinterOne.setFlowPlanNum(0);
				sinterOne.setInNum(0);
				sinterOne.setInPlanNum(0);
				sinterOne.setInPutNum(0);
				sinterOne.setInPutPlanNum(0);
				sinterOne.setOutNum(0);
				sinterOne.setOutPlanNum(0);
				sinterOne.setOutPutNum(0);
				sinterOne.setOutPutPlanNum(0);
				sinterOne.setOxygenNum(0);
				sinterOne.setOxygenPlanNum(0);
				sinterOne.setScrapNum(0);

				List<SinterOne> sinterOneList = new ArrayList<>();
				
				CheckStatisticsQO qoc = new CheckStatisticsQO();
				qoc = qo;
				qoc.setProductionLineId(procedure.getProductionLineId());
				Integer planNum = fireCheckStatisticsMapper.selectSinerNum(qoc);
				
				int index = 0;
				
				if(!CollectionUtils.isEmpty(item.getSinterOneList())){
					sinterOneList = item.getSinterOneList();
					for (SinterOne bowl : sinterOneList) {
						index++;
						if(bowl.getProductionLineName().equals(sinterOne.getProductionLineName())) {
							sinterOne = bowl;
						}
					}
				}else {
					sinterOneList.add(sinterOne);
				}
				qoc.setProductionLineId(procedure.getProductionLineId());
				int inPlanNum =fireCheckStatisticsMapper.selectSinerNum(qoc);

				TrafficPressureSubDTO dto = new TrafficPressureSubDTO();
				dto.setProcedureOrderId(procedure.getId());
				Long countIncludeMainByQuery = trafficPressureSubMapper.countIncludeMainByQuery(dto);
				ParameterSubDTO paraDto = new ParameterSubDTO();
				paraDto.setProcedureOrderId(procedure.getId());
				int paramSize = parameterSubService.countIncludeMainByQuery(paraDto).intValue();
				
				Integer inSize = ObjectUtils.isEmpty(inMap.get(procedure.getId()))?0:inMap.get(procedure.getId()).getProcedureSize();
				Integer outSize = ObjectUtils.isEmpty(outMap.get(procedure.getId()))?0:outMap.get(procedure.getId()).getProcedureSize();
				Integer inputSize = ObjectUtils.isEmpty(inputMap.get(procedure.getId()))?0:inputMap.get(procedure.getId()).getProcedureSize();
				Integer outputSize = ObjectUtils.isEmpty(outputMap.get(procedure.getId()))?0:outputMap.get(procedure.getId()).getProcedureSize();
				Integer scarpSize = ObjectUtils.isEmpty(scparpMap.get(procedure.getId()))?0:scparpMap.get(procedure.getId()).getProcedureSize();
				Integer csmSize = ObjectUtils.isEmpty(csmMap.get(procedure.getId()))?0:csmMap.get(procedure.getId()).getProcedureSize();
				Integer acmSize = ObjectUtils.isEmpty(acmMap.get(procedure.getId()))?0:acmMap.get(procedure.getId()).getProcedureSize();
				
				sinterOne.setInPlanNum(inPlanNum);
				sinterOne.setOutPlanNum(inPlanNum);
				
				sinterOne.setInNum(sinterOne.getInNum()+inSize);
				sinterOne.setOutNum(sinterOne.getOutNum()+outSize);
				sinterOne.setInPlanNum(days.intValue()*2);
				sinterOne.setInPutNum(inputSize+sinterOne.getInPutNum());
				sinterOne.setOutPutPlanNum(days.intValue()*2);
				sinterOne.setOutPutNum(outputSize+sinterOne.getOutPutNum());
				sinterOne.setFlowPlanNum(countIncludeMainByQuery.intValue()*(24/4)+sinterOne.getFlowPlanNum());
				sinterOne.setFlowNum(countIncludeMainByQuery.intValue()+sinterOne.getFlowNum());
				sinterOne.setOxygenNum(paramSize+sinterOne.getOxygenNum());
				sinterOne.setOxygenPlanNum(paramSize*(24/4)+sinterOne.getOxygenPlanNum());
				sinterOne.setScrapNum(sinterOne.getScrapNum()+scarpSize);
				sinterOne.setCsmNum(sinterOne.getCsmNum()+csmSize);
				sinterOne.setAcmNum(acmSize+sinterOne.getAcmNum());
				
				sinterOneList.set(index, sinterOne);
				item.setSinterOneList(sinterOneList);
				
			});
		});
	}
	
	public void dealWithSinterTwo(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo) {
		if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}

		CheckStatisticsQO qos = new CheckStatisticsQO();
		qos = qo;
		LocalDate startDate =qo.getStartTime();
		LocalDate endDate = qo.getEndTime();
		Duration duration = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay());
		Long days = duration.toDays(); // 计算天数

		qos.setProduceNumber(2);
		qos.setTableName("kiln_material_in_sub");
		List<ProcedureSumVO> selectSinerNumber = fireCheckStatisticsMapper.selectSinerNumber(qos);
		
		qos.setTableName("kiln_material_out_sub");
		List<ProcedureSumVO> outList = fireCheckStatisticsMapper.selectSinerNumber(qos);
		
		qos.setTableName("produce_record");
		qos.setDynamicSQL("and category = '进窑'");
		List<ProcedureSumVO> inputList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		qos.setDynamicSQL("and category = '出窑'");
		List<ProcedureSumVO> outputList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		qos.setTableName("rotary_parameter");
		qos.setDynamicSQL(null);
		List<ProcedureSumVO> rotaryList = fireCheckStatisticsMapper.selectTableNameByProcedure(qos);
		
		//rotary_parameter
		
		HashMap<Integer, ProcedureSumVO> inMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> outMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> outputMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> inputMap = new HashMap<>();
		HashMap<Integer, ProcedureSumVO> rotaryMap = new HashMap<>();
		
		for (ProcedureSumVO vo : selectSinerNumber) {
			inMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : outList) {
			outMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : outputList) {
			outputMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : inputList) {
			inputMap.put(vo.getProductionLineId(), vo);
		}
		
		for (ProcedureSumVO vo : rotaryList) {
			rotaryMap.put(vo.getProductionLineId(), vo);
		}
		
				resultMap.values().stream().forEach( item ->{
					orderList.stream()
					.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
					.forEach(procedure->{
						SinterTwo sinterTwo = new FireCheckStatistics().new SinterTwo();
						sinterTwo.setWorkshopName(procedure.getWorkshopName());
						sinterTwo.setProductionLineName(procedure.getProductionLine());
						sinterTwo.setInNum(0);
						sinterTwo.setInPlanNum(0);
						sinterTwo.setInPutNum(0);
						sinterTwo.setInPutPlanNum(0);
						sinterTwo.setKilnNum(0);
						sinterTwo.setKilnPlanNum(0);
						sinterTwo.setOutNum(0);
						sinterTwo.setOutPlanNum(0);
						sinterTwo.setOutPutNum(0);
						sinterTwo.setOutPutPlanNum(0);
						
						List<SinterTwo> sinterTwoList = new ArrayList<>();
						
						int index = 0;
						
						if(!CollectionUtils.isEmpty(item.getSinterTwo())){
							sinterTwoList = item.getSinterTwo();
							for (SinterTwo bowl : sinterTwoList) {
								index++;
								if(bowl.getProductionLineName().equals(sinterTwo.getProductionLineName())) {
									sinterTwo = bowl;
								}
							}
						}else {
							sinterTwoList.add(sinterTwo);
						}

						CheckStatisticsQO qoc = new CheckStatisticsQO();
						qoc = qo;
						qoc.setProductionLineId(procedure.getProductionLineId());
						int inPlanNum =fireCheckStatisticsMapper.selectSinerNum(qoc);
						

						Integer inSize = ObjectUtils.isEmpty(inMap.get(procedure.getId()))?0:inMap.get(procedure.getId()).getProcedureSize();
						Integer outSize = ObjectUtils.isEmpty(outMap.get(procedure.getId()))?0:outMap.get(procedure.getId()).getProcedureSize();
						Integer inputSize = ObjectUtils.isEmpty(inputMap.get(procedure.getId()))?0:inputMap.get(procedure.getId()).getProcedureSize();
						Integer outputSize = ObjectUtils.isEmpty(outputMap.get(procedure.getId()))?0:outputMap.get(procedure.getId()).getProcedureSize();
						Integer rotarySize = ObjectUtils.isEmpty(rotaryMap.get(procedure.getId()))?0:rotaryMap.get(procedure.getId()).getProcedureSize();
						
						sinterTwo.setInPlanNum(inPlanNum);
						sinterTwo.setOutPlanNum(inPlanNum);
						
						sinterTwo.setInNum(sinterTwo.getInNum()+inSize);
						sinterTwo.setOutNum(sinterTwo.getOutNum()+outSize);
						sinterTwo.setInPlanNum(days.intValue()*2);
						sinterTwo.setInPutNum(inputSize+sinterTwo.getInPutNum());
						sinterTwo.setOutPutPlanNum(days.intValue()*2);
						sinterTwo.setOutPutNum(outputSize+sinterTwo.getOutPutNum());
						sinterTwo.setKilnPlanNum(rotarySize*(24/4));
						sinterTwo.setKilnNum(rotarySize+sinterTwo.getKilnNum());
						
						sinterTwoList.set(index, sinterTwo);
						item.setSinterTwo(sinterTwoList);
						
					});
				});
	}

	// ============计算混批
	public void dealWithMixedBatch(ArrayList<ProcedureVO> orderList, LinkedHashMap<String, FireCheckStatistics> resultMap,CheckStatisticsQO qo) {
		if(orderList==null) {
			log.info("{},{}",orderList,orderList==null);
			return;
		}

		//火法包装混批
		qo.setTableName("mixed_batch");
		List<ProcedureSumVO> selectTableNameByProcedure = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
		qo.setTableName("mixed_record");
		
		List<ProcedureSumVO> mixrecord = fireCheckStatisticsMapper.selectTableNameByProcedure(qo);
		
		
		Map<Integer, ProcedureSumVO> map = new HashMap<>();
		for (ProcedureSumVO procedureSumVO : selectTableNameByProcedure) {
		    map.put(procedureSumVO.getId(), procedureSumVO);
		}
		
		Map<Integer, ProcedureSumVO> mixrecordMap = new HashMap<>();
		for (ProcedureSumVO procedureSumVO : mixrecord) {
			mixrecordMap.put(procedureSumVO.getId(), procedureSumVO);
		}

		
		resultMap.values().stream().forEach( item ->{
			orderList.stream()
			.filter(procedure -> procedure.getWorkshopId() == item.getWorkshopId())
			.forEach(procedure->{
				MixedBatch mixedBatch = new FireCheckStatistics().new MixedBatch();
				mixedBatch.setWorkshopName(procedure.getWorkshopName());
				mixedBatch.setProductionLineName(procedure.getProductionLine());
				mixedBatch.setMixedNum(0);
				mixedBatch.setMixedPlanNum(0);
				mixedBatch.setNum(0);
				mixedBatch.setPlanNum(0);
				List<MixedBatch> mixedBatchList = new ArrayList<>();
				
				int index = 0;
				
				if(!CollectionUtils.isEmpty(item.getMixedBatchList())){
					mixedBatchList = item.getMixedBatchList();
					for (MixedBatch bowl : mixedBatchList) {
						index++;
						if(bowl.getProductionLineName().equals(mixedBatch.getProductionLineName())) {
							mixedBatch = bowl;
						}
					}
				}else {
					mixedBatchList.add(mixedBatch);
				}
				
				 Integer num = ObjectUtils.isEmpty(map.get(procedure.getId()))?0:map.get(procedure.getId()).getProcedureSize();
				
				 Integer reocrdSize = ObjectUtils.isEmpty(mixrecordMap.get(procedure.getId()))?0:mixrecordMap.get(procedure.getId()).getProcedureSize();
				
				if(procedure.getProductionScheduleStatus()!=null&&procedure.getProductionScheduleStatus()==20) {
					mixedBatch.setPlanNum(num+mixedBatch.getNum());
					mixedBatch.setMixedNum(reocrdSize+mixedBatch.getMixedNum());
				}
				mixedBatch.setPlanNum(num+mixedBatch.getPlanNum());
				
				mixedBatch.setMixedPlanNum(reocrdSize+mixedBatch.getMixedPlanNum());

				mixedBatchList.set(index, mixedBatch);
				item.setMixedBatchList(mixedBatchList);
				
			});
		});
	}
}
