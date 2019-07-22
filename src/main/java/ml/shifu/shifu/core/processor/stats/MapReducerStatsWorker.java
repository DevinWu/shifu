/*
 * Copyright [2013-2015] PayPal Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.shifu.core.processor.stats;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.Lists;
import ml.shifu.shifu.fs.SourceFile;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.pig.impl.util.JarManager;
import org.encog.ml.data.MLDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;

import ml.shifu.guagua.hadoop.util.HDPUtils;
import ml.shifu.guagua.mapreduce.GuaguaMapReduceConstants;
import ml.shifu.guagua.util.FileUtils;
import ml.shifu.shifu.container.obj.ColumnConfig;
import ml.shifu.shifu.container.obj.ColumnConfig.ColumnFlag;
import ml.shifu.shifu.container.obj.ColumnType;
import ml.shifu.shifu.container.obj.ModelConfig;
import ml.shifu.shifu.container.obj.RawSourceData;
import ml.shifu.shifu.container.obj.RawSourceData.SourceType;
import ml.shifu.shifu.core.binning.BinningInfoWritable;
import ml.shifu.shifu.core.binning.UpdateBinningInfoMapper;
import ml.shifu.shifu.core.binning.UpdateBinningInfoReducer;
import ml.shifu.shifu.core.dtrain.CommonConstants;
import ml.shifu.shifu.core.dtrain.nn.NNConstants;
import ml.shifu.shifu.core.mr.input.CombineInputFormat;
import ml.shifu.shifu.core.processor.BasicModelProcessor;
import ml.shifu.shifu.exception.ShifuErrorCode;
import ml.shifu.shifu.exception.ShifuException;
import ml.shifu.shifu.fs.PathFinder;
import ml.shifu.shifu.fs.ShifuFileUtils;
import ml.shifu.shifu.pig.PigExecutor;
import ml.shifu.shifu.udf.CalculateStatsUDF;
import ml.shifu.shifu.util.Base64Utils;
import ml.shifu.shifu.util.CommonUtils;
import ml.shifu.shifu.util.Constants;
import ml.shifu.shifu.util.Environment;
import ml.shifu.shifu.util.ValueVisitor;
import ml.shifu.shifu.core.datestat.DateStatComputeMapper;
import ml.shifu.shifu.core.datestat.DateStatComputeReducer;
import ml.shifu.shifu.core.datestat.DateStatInfoWritable;

import static ml.shifu.shifu.util.Constants.LOCAL_DATE_STATS_CSV_FILE_NAME;

/**
 * Created by zhanhu on 6/30/16.
 */
public class MapReducerStatsWorker extends AbstractStatsExecutor {

    private static Logger log = LoggerFactory.getLogger(MapReducerStatsWorker.class);

    public static final Long MINIMUM_DISTINCT_CNT = 2L;
    public static final Long MAXIMUM_DISTINCT_CNT = 1000L;

    protected PathFinder pathFinder = null;

    public MapReducerStatsWorker(BasicModelProcessor processor, ModelConfig modelConfig,
            List<ColumnConfig> columnConfigList) {
        super(processor, modelConfig, columnConfigList);
        pathFinder = processor.getPathFinder();
    }

    @Override
    public boolean doStats() throws Exception {
        log.info("delete historical pre-train data");

        if(this.modelConfig.isMultiTask()) {
            ShifuFileUtils.deleteFile(pathFinder.getPreTrainingStatsPath(this.getMtlIndex()),
                    modelConfig.getDataSet().getSource());
        } else {
            ShifuFileUtils.deleteFile(pathFinder.getPreTrainingStatsPath(), modelConfig.getDataSet().getSource());
        }
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("delimiter", CommonUtils.escapePigString(modelConfig.getDataSetDelimiter()));

        int columnParallel = getColumnParallelNum();
        paramsMap.put("column_parallel", Integer.toString(columnParallel));

        paramsMap.put("histo_scale_factor", Environment.getProperty("shifu.stats.histo.scale.factor", "100"));

        try {
            runStatsPig(paramsMap);
        } catch (IOException e) {
            throw new ShifuException(ShifuErrorCode.ERROR_RUNNING_PIG_JOB, e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        // sync Down
        log.info("Updating ColumnConfig with stats...");
        // update column config
        updateColumnConfigWithPreTrainingStats();

        // check categorical columns and numerical columns and warning
        checkNumericalAndCategoricalColumns();

        // save it to local/hdfs
        saveAndSyncColumnConfigs();

        boolean toRunPSIWithStats = Environment.getBoolean("shifu.stats.psi.together", true);
        if(toRunPSIWithStats && StringUtils.isNotEmpty(modelConfig.getPsiColumnName())) {
            runPSI();
            saveAndSyncColumnConfigs();
        }

        // run daily stat compute
        updateDateStatWithMRJob();

        return true;
    }

    private int getColumnParallelNum() throws IOException {
        // this is only roughly estimation to get mapper size
        Configuration conf = new Configuration();
        CommonUtils.injectHadoopShifuEnvironments(new ValueVisitor() {
            @Override
            public void inject(Object key, Object value) {
                conf.set(key.toString(), value.toString());
            }
        });
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Shifu: Only for Mapper Size Estimation");
        FileInputFormat.setInputPaths(job,
                ShifuFileUtils.getFileSystemBySourceType(modelConfig.getDataSet().getSource())
                        .makeQualified(new Path(super.modelConfig.getDataSetRawPath())));
        int mapperSize = new CombineInputFormat().getSplits(job).size();
        log.info("Estimated mapper size is {}.", mapperSize);

        int columnParallel = 0;
        if(columnConfigList.size() <= 100) {
            columnParallel = columnConfigList.size() * 2;
        } else if(columnConfigList.size() <= 500) {
            columnParallel = columnConfigList.size();
        } else if(columnConfigList.size() <= 1000) {
            // 1000 => 200 reducers
            columnParallel = columnConfigList.size() / 2;
        } else if(columnConfigList.size() > 1000 && columnConfigList.size() <= 2000) {
            // 2000 => 320 reducers
            columnParallel = columnConfigList.size() / 4;
        } else if(columnConfigList.size() > 2000 && columnConfigList.size() <= 3000) {
            // 3000 => 420 reducers
            columnParallel = columnConfigList.size() / 6;
        } else if(columnConfigList.size() > 3000 && columnConfigList.size() <= 4000) {
            // 4000 => 500
            columnParallel = columnConfigList.size() / 8;
        } else {
            // 5000 => 500
            columnParallel = columnConfigList.size() / 10;
        }

        // if too small mapper size, reset it to a smaller one to avoid big # of reducers.
        if(mapperSize <= 20) {
            columnParallel = Math.min(mapperSize, columnParallel);
        }

        // limit max reducer to 999
        int parallelNumbByVolume = getParallelNumByDataVolume();
        if(columnParallel < parallelNumbByVolume) {
            columnParallel = parallelNumbByVolume;
            log.info("Adjust parallel number to {} according data volume", columnParallel);
        }
        columnParallel = columnParallel > 999 ? 999 : columnParallel;
        return columnParallel;
    }

    private void saveAndSyncColumnConfigs() throws IOException {
        if(this.modelConfig.isMultiTask()) {
            String ccPath = new PathFinder(this.modelConfig).getMTLColumnConfigPath(SourceType.LOCAL,
                    this.getMtlIndex());
            processor.saveColumnConfigList(ccPath, columnConfigList);
        } else {
            processor.saveColumnConfigList();
        }
        processor.syncDataToHdfs(modelConfig.getDataSet().getSource());
    }

    private int getParallelNumByDataVolume() throws IOException {
        long fileSize = ShifuFileUtils.getFileOrDirectorySize(modelConfig.getDataSet().getDataPath(),
                modelConfig.getDataSet().getSource());
        log.info("File Size is - {}, for {}", fileSize, modelConfig.getDataSet().getDataPath());
        if(ShifuFileUtils.isCompressedFileOrDirectory(modelConfig.getDataSet().getDataPath(),
                modelConfig.getDataSet().getSource())) {
            log.info("File is compressed, for {}", modelConfig.getDataSet().getDataPath());
            fileSize = fileSize * 3; // multi 3 times, if the file is compressed
        }
        return (int) (fileSize / (256 * 1024 * 1024l)); // each reducer handle 256MB data
    }

    /**
     * According to sample values and distinct count in each column, check if user set wrong for numerical and
     * categorical features. Only warning message are output to console for user to check.
     */
    private void checkNumericalAndCategoricalColumns() {
        for(ColumnConfig config: this.columnConfigList) {
            if(config != null && !config.isMeta() && !config.isTarget()) {
                List<String> sampleValues = config.getSampleValues();
                if(config.isNumerical() && sampleValues != null) {
                    int nums = numberCount(sampleValues);
                    if((nums * 1d / sampleValues.size()) < 0.5d) {
                        log.warn(
                                "Column {} with index {} is set to numrical but numbers are less than 50% in ColumnConfig::SampleValues, please check if it is numerical feature.",
                                config.getColumnName(), config.getColumnNum());
                    }
                }

                if(config.isCategorical() && sampleValues != null) {
                    int nums = numberCount(sampleValues);
                    if((nums * 1d / sampleValues.size()) > 0.95d && config.getColumnStats().getDistinctCount() != null
                            && config.getColumnStats().getDistinctCount() > 5000) {
                        log.warn(
                                "Column {} with index {} is set to categorical but numbers are more than 95% in ColumnConfig::SampleValues and distinct count is over 5000, please check if it is categorical feature.",
                                config.getColumnName(), config.getColumnNum());
                    }
                }
            }
        }
    }

    private int numberCount(List<String> sampleValues) {
        int numbers = 0;
        for(String str: sampleValues) {
            try {
                Double.parseDouble(str);
                numbers += 1;
            } catch (Exception ignore) {
            }
        }
        return numbers;
    }

    protected void runStatsPig(Map<String, String> paramsMap) throws Exception {
        paramsMap.put("group_binning_parallel", Integer.toString(columnConfigList.size() / (5 * 8)));

        if(this.modelConfig.isMultiTask()) {
            ShifuFileUtils.deleteFile(
                    pathFinder.getUpdatedBinningInfoPath(modelConfig.getDataSet().getSource(), this.getMtlIndex()),
                    modelConfig.getDataSet().getSource());
            paramsMap.put(CommonConstants.MTL_INDEX, this.getMtlIndex() + "");
        } else {
            ShifuFileUtils.deleteFile(pathFinder.getUpdatedBinningInfoPath(modelConfig.getDataSet().getSource()),
                    modelConfig.getDataSet().getSource());
        }

        log.debug("this.pathFinder.getOtherConfigs() => " + this.pathFinder.getOtherConfigs());
        PigExecutor.getExecutor().submitJob(modelConfig, pathFinder.getScriptPath("scripts/StatsSpdtI.pig"), paramsMap,
                modelConfig.getDataSet().getSource(), this.pathFinder);
        // update
        log.info("Updating binning info ...");
        updateBinningInfoWithMRJob();
    }

    protected void updateDateStatWithMRJob() throws IOException, InterruptedException, ClassNotFoundException {
        if(StringUtils.isEmpty(this.modelConfig.getDateColumnName())) {
            log.info("Date column name is not set in ModelConfig file, will cancel updateDateStatWithMRJob.");
            return;
        }

        RawSourceData.SourceType source = this.modelConfig.getDataSet().getSource();

        Configuration conf = new Configuration();
        prepareJobConf(source, conf, null);

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Shifu: Date Stats Job : " + this.modelConfig.getModelSetName());
        job.setJarByClass(getClass());
        job.setMapperClass(DateStatComputeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DateStatInfoWritable.class);
        job.setInputFormatClass(CombineInputFormat.class);
        FileInputFormat.setInputPaths(job, ShifuFileUtils.getFileSystemBySourceType(source)
                .makeQualified(new Path(super.modelConfig.getDataSetRawPath())));

        job.setReducerClass(DateStatComputeReducer.class);

        int mapperSize = new CombineInputFormat().getSplits(job).size();
        log.info("DEBUG: Test mapper size is {} ", mapperSize);
        Integer reducerSize = Environment.getInt(CommonConstants.SHIFU_DAILYSTAT_REDUCER);
        if(reducerSize != null) {
            job.setNumReduceTasks(Environment.getInt(CommonConstants.SHIFU_DAILYSTAT_REDUCER, 20));
        } else {
            // By average, each reducer handle 100 variables
            int newReducerSize = (this.columnConfigList.size() / 100) + 1;
            log.info("Adjust date stat info reducer size to {} ", newReducerSize);
            job.setNumReduceTasks(newReducerSize);
        }
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String preTrainingInfo = this.pathFinder.getPreTrainingStatsPath(source);
        Path path = new Path(preTrainingInfo);
        log.info("Output path:" + path);
        FileOutputFormat.setOutputPath(job, path);

        // clean output firstly
        ShifuFileUtils.deleteFile(preTrainingInfo, source);

        // submit job
        if(!job.waitForCompletion(true)) {
            throw new RuntimeException("MapReduce Job Updating date stat Info failed.");
        } else {
            long totalValidCount = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "TOTAL_VALID_COUNT")
                    .getValue();
            long invalidTagCount = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "INVALID_TAG")
                    .getValue();
            long filterOut = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "FILTER_OUT_COUNT")
                    .getValue();
            long weightExceptions = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "WEIGHT_EXCEPTION")
                    .getValue();
            log.info(
                    "Total valid records {}, invalid tag records {}, filter out records {}, weight exception records {}",
                    totalValidCount, invalidTagCount, filterOut, weightExceptions);

            if(totalValidCount > 0L && invalidTagCount * 1d / totalValidCount >= 0.8d) {
                log.warn("Too many invalid tags, please check you configuration on positive tags and negative tags.");
            }
            copyFileToLocal(conf, path);
        }
    }

    private void copyFileToLocal(Configuration conf, Path path) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = hdfs.listFiles(path, false);
        List<Path> list = new ArrayList<>();
        while(locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path p = next.getPath();
            if(p.getName().endsWith("gz")) {
                list.add(p);
            }
        }
        Collections.sort(list, new Comparator<Path>() {

            private Integer getDigitInPath(String path) {
                String resultStr = StringUtils.substringBefore(StringUtils.substringAfterLast(path, "-"), ".");
                if(StringUtils.isEmpty(resultStr)) {
                    return 0;
                }
                return Integer.parseInt(resultStr);
            }

            @Override
            public int compare(Path o1, Path o2) {
                return getDigitInPath(o1.getName()) - getDigitInPath(o2.getName());
            }
        });
        String dateStatsOutputFileName = this.modelConfig.getStats().getDateStatsOutputFileName();
        File file = new File(StringUtils.isEmpty(dateStatsOutputFileName) ? LOCAL_DATE_STATS_CSV_FILE_NAME
                : dateStatsOutputFileName);
        OutputStream out = org.apache.commons.io.FileUtils.openOutputStream(file);
        for(Path p: list) {
            FSDataInputStream in = hdfs.open(p);
            GZIPInputStream gzin = new GZIPInputStream(in);
            IOUtils.copy(gzin, out);
            IOUtils.closeQuietly(gzin);
        }
        IOUtils.closeQuietly(out);
        log.info("Copy file to local:" + file.getAbsolutePath());
    }

    protected void updateBinningInfoWithMRJob() throws IOException, InterruptedException, ClassNotFoundException {
        RawSourceData.SourceType source = this.modelConfig.getDataSet().getSource();

        String filePath = Constants.BINNING_INFO_FILE_NAME;
        BufferedWriter writer = null;
        List<Scanner> scanners = null;
        try {
            if(this.modelConfig.isMultiTask()) {
                scanners = ShifuFileUtils
                        .getDataScanners(pathFinder.getUpdatedBinningInfoPath(source, this.getMtlIndex()), source);
                filePath = Constants.BINNING_INFO_FILE_NAME + "." + this.getMtlIndex();
            } else {
                scanners = ShifuFileUtils.getDataScanners(pathFinder.getUpdatedBinningInfoPath(source), source);
                filePath = Constants.BINNING_INFO_FILE_NAME;
            }
            writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(new File(filePath)), Charset.forName("UTF-8")));
            for(Scanner scanner: scanners) {
                while(scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    writer.write(line + "\n");
                }
            }
        } finally {
            // release
            processor.closeScanners(scanners);
            IOUtils.closeQuietly(writer);
        }

        Configuration conf = new Configuration();
        prepareJobConf(source, conf, filePath);
        conf.set(CommonConstants.MTL_INDEX, this.getMtlIndex() + "");

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Shifu: Stats Updating Binning Job : " + this.modelConfig.getModelSetName());
        job.setJarByClass(getClass());
        job.setMapperClass(UpdateBinningInfoMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BinningInfoWritable.class);
        job.setInputFormatClass(CombineInputFormat.class);
        FileInputFormat.setInputPaths(job, ShifuFileUtils.getFileSystemBySourceType(source)
                .makeQualified(new Path(super.modelConfig.getDataSetRawPath())));

        job.setReducerClass(UpdateBinningInfoReducer.class);

        int mapperSize = new CombineInputFormat().getSplits(job).size();
        log.info("DEBUG: Test mapper size is {} ", mapperSize);
        Integer reducerSize = Environment.getInt(CommonConstants.SHIFU_UPDATEBINNING_REDUCER);
        if(reducerSize != null) {
            job.setNumReduceTasks(Environment.getInt(CommonConstants.SHIFU_UPDATEBINNING_REDUCER, 20));
        } else {
            // By average, each reducer handle 100 variables
            int newReducerSize = (this.columnConfigList.size() / 100) + 1;
            log.info("Adjust updating binning info reducer size to {} ", newReducerSize);
            job.setNumReduceTasks(newReducerSize);
        }
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String preTrainingInfo;
        if(this.modelConfig.isMultiTask()) {
            preTrainingInfo = this.pathFinder.getPreTrainingStatsPath(source, this.getMtlIndex());
            FileOutputFormat.setOutputPath(job, new Path(preTrainingInfo));
        } else {
            preTrainingInfo = this.pathFinder.getPreTrainingStatsPath(source);
            FileOutputFormat.setOutputPath(job, new Path(preTrainingInfo));
        }

        // clean output firstly
        ShifuFileUtils.deleteFile(preTrainingInfo, source);

        // submit job
        if(!job.waitForCompletion(true)) {
            FileUtils.deleteQuietly(new File(filePath));
            throw new RuntimeException("MapReduce Job Updateing Binning Info failed.");
        } else {
            long totalValidCount = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "TOTAL_VALID_COUNT")
                    .getValue();
            long invalidTagCount = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "INVALID_TAG")
                    .getValue();
            long filterOut = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "FILTER_OUT_COUNT")
                    .getValue();
            long weightExceptions = job.getCounters().findCounter(Constants.SHIFU_GROUP_COUNTER, "WEIGHT_EXCEPTION")
                    .getValue();
            log.info(
                    "Total valid records {}, invalid tag records {}, filter out records {}, weight exception records {}",
                    totalValidCount, invalidTagCount, filterOut, weightExceptions);

            if(totalValidCount > 0L && invalidTagCount * 1d / totalValidCount >= 0.8d) {
                log.warn("Too many invalid tags, please check you configuration on positive tags and negative tags.");
            }
        }
        FileUtils.deleteQuietly(new File(filePath));
    }

    private void prepareJobConf(RawSourceData.SourceType source, final Configuration conf, String filePath)
            throws IOException {
        // add jars to hadoop mapper and reducer
        if(StringUtils.isNotEmpty(filePath)) {
            new GenericOptionsParser(conf, new String[] { "-libjars", addRuntimeJars(), "-files", filePath });
        } else {
            new GenericOptionsParser(conf, new String[] { "-libjars", addRuntimeJars() });
        }

        conf.setBoolean(CombineInputFormat.SHIFU_VS_SPLIT_COMBINABLE, true);
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        conf.set(Constants.SHIFU_STATS_EXLCUDE_MISSING,
                Environment.getProperty(Constants.SHIFU_STATS_EXLCUDE_MISSING, "true"));

        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION, true);
        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION, true);
        conf.setBoolean(GuaguaMapReduceConstants.MAPREDUCE_MAP_SPECULATIVE, true);
        conf.setBoolean(GuaguaMapReduceConstants.MAPREDUCE_REDUCE_SPECULATIVE, true);
        conf.set(Constants.SHIFU_MODEL_CONFIG, ShifuFileUtils.getFileSystemBySourceType(source)
                .makeQualified(new Path(this.pathFinder.getModelConfigPath(source))).toString());
        conf.set(Constants.SHIFU_COLUMN_CONFIG, ShifuFileUtils.getFileSystemBySourceType(source)
                .makeQualified(new Path(this.pathFinder.getColumnConfigPath(source))).toString());
        conf.set(NNConstants.MAPRED_JOB_QUEUE_NAME, Environment.getProperty(Environment.HADOOP_JOB_QUEUE, "default"));
        conf.set(Constants.SHIFU_MODELSET_SOURCE_TYPE, source.toString());

        // set mapreduce.job.max.split.locations to 30 to suppress warnings
        conf.setInt(GuaguaMapReduceConstants.MAPREDUCE_JOB_MAX_SPLIT_LOCATIONS, 5000);
        conf.set("mapred.reduce.slowstart.completed.maps",
                Environment.getProperty("mapred.reduce.slowstart.completed.maps", "0.8"));

        conf.set(Constants.SHIFU_STATS_FILTER_EXPRESSIONS, super.modelConfig.getSegmentFilterExpressionsAsString());
        log.info("segment expressions is {}", super.modelConfig.getSegmentFilterExpressionsAsString());

        String hdpVersion = HDPUtils.getHdpVersionForHDP224();
        if(StringUtils.isNotBlank(hdpVersion)) {
            // for hdp 2.2.4, hdp.version should be set and configuration files should be add to container class path
            conf.set("hdp.version", hdpVersion);
        }

        // one can set guagua conf in shifuconfig
        CommonUtils.injectHadoopShifuEnvironments(new ValueVisitor() {
            @Override
            public void inject(Object key, Object value) {
                conf.set(key.toString(), value.toString());
            }
        });
    }

    // GuaguaOptionsParser doesn't to support *.jar currently.
    private String addRuntimeJars() {
        List<String> jars = new ArrayList<String>(16);
        // common-codec
        jars.add(JarManager.findContainingJar(Base64.class));
        // commons-compress-*.jar
        jars.add(JarManager.findContainingJar(BZip2CompressorInputStream.class));
        // commons-lang-*.jar
        jars.add(JarManager.findContainingJar(StringUtils.class));
        // common-io-*.jar
        jars.add(JarManager.findContainingJar(org.apache.commons.io.IOUtils.class));
        // common-collections
        jars.add(JarManager.findContainingJar(Predicate.class));
        // guava-*.jar
        jars.add(JarManager.findContainingJar(Splitter.class));
        // shifu-*.jar
        jars.add(JarManager.findContainingJar(getClass()));
        // jexl
        jars.add(JarManager.findContainingJar(JexlException.class));
        // encog-core-*.jar
        jars.add(JarManager.findContainingJar(MLDataSet.class));
        // jackson-databind-*.jar
        jars.add(JarManager.findContainingJar(ObjectMapper.class));
        // jackson-core-*.jar
        jars.add(JarManager.findContainingJar(JsonParser.class));
        // jackson-annotations-*.jar
        jars.add(JarManager.findContainingJar(JsonIgnore.class));
        // stream-llib-*.jar
        jars.add(JarManager.findContainingJar(HyperLogLogPlus.class));

        return StringUtils.join(jars, NNConstants.LIB_JAR_SEPARATOR);
    }

    /**
     * update the max/min/mean/std/binning information from stats step
     * 
     * @throws IOException
     *             in stats processing from hdfs files
     */
    public void updateColumnConfigWithPreTrainingStats() throws IOException {
        List<Scanner> scanners;
        if(this.modelConfig.isMultiTask()) {
            scanners = ShifuFileUtils.getDataScanners(pathFinder.getPreTrainingStatsPath(this.getMtlIndex()),
                    modelConfig.getDataSet().getSource());
        } else {
            scanners = ShifuFileUtils.getDataScanners(pathFinder.getPreTrainingStatsPath(),
                    modelConfig.getDataSet().getSource());
        }
        int initSize = columnConfigList.size();
        for(Scanner scanner: scanners) {
            scanStatsResult(scanner, initSize);
        }
        // release
        processor.closeScanners(scanners);

        Collections.sort(this.columnConfigList, new Comparator<ColumnConfig>() {
            @Override
            public int compare(ColumnConfig o1, ColumnConfig o2) {
                return o1.getColumnNum().compareTo(o2.getColumnNum());
            }
        });
    }

    /**
     * Scan the stats result and save them into column configure
     * 
     * @param scanner
     *            the scanners to be read
     */
    private void scanStatsResult(Scanner scanner, int ccInitSize) {
        while(scanner.hasNextLine()) {
            String[] raw = scanner.nextLine().trim().split("\\|");

            if(raw.length == 1) {
                continue;
            }

            if(raw.length < 25) {
                log.info("The stats data has " + raw.length + " fields.");
                log.info("The stats data is - " + Arrays.toString(raw));
            }

            int columnNum = Integer.parseInt(raw[0]);

            int corrColumnNum = columnNum;
            if(columnNum >= ccInitSize) {
                corrColumnNum = columnNum % ccInitSize;
            }

            try {
                ColumnConfig basicConfig = this.columnConfigList.get(corrColumnNum);
                log.debug("basicConfig is - " + basicConfig.getColumnName() + " corrColumnNum:" + corrColumnNum);

                ColumnConfig config = null;
                if(columnNum >= ccInitSize) {
                    config = new ColumnConfig();
                    config.setColumnNum(columnNum);
                    config.setColumnName(basicConfig.getColumnName() + "_" + (columnNum / ccInitSize));
                    config.setVersion(basicConfig.getVersion());
                    config.setColumnType(basicConfig.getColumnType());
                    config.setColumnFlag(basicConfig.getColumnFlag() == ColumnFlag.Target ? ColumnFlag.Meta
                            : basicConfig.getColumnFlag());

                    log.debug("basicConfig is - " + basicConfig.getColumnName() + " corrColumnNum:" + corrColumnNum
                            + ", currColumnName: " + columnNum + ", currColumnType:" + config.getColumnType());

                    this.columnConfigList.add(config);
                } else {
                    config = basicConfig;
                }

                if(config.isHybrid()) {
                    String[] splits = CommonUtils.split(raw[1], Constants.HYBRID_BIN_STR_DILIMETER);
                    config.setBinBoundary(CommonUtils.stringToDoubleList(splits[0]));
                    String binCategory = Base64Utils.base64Decode(splits[1]);
                    config.setBinCategory(
                            CommonUtils.stringToStringList(binCategory, CalculateStatsUDF.CATEGORY_VAL_SEPARATOR));
                } else if(config.isCategorical()) {
                    String binCategory = Base64Utils.base64Decode(raw[1]);
                    config.setBinCategory(
                            CommonUtils.stringToStringList(binCategory, CalculateStatsUDF.CATEGORY_VAL_SEPARATOR));
                    config.setBinBoundary(null);
                } else {
                    config.setBinBoundary(CommonUtils.stringToDoubleList(raw[1]));
                    config.setBinCategory(null);
                }
                config.setBinCountNeg(CommonUtils.stringToIntegerList(raw[2]));
                config.setBinCountPos(CommonUtils.stringToIntegerList(raw[3]));
                // config.setBinAvgScore(CommonUtils.stringToIntegerList(raw[4]));
                config.setBinPosCaseRate(CommonUtils.stringToDoubleList(raw[5]));
                config.setBinLength(config.getBinCountNeg().size());
                config.setKs(parseDouble(raw[6]));
                config.setIv(parseDouble(raw[7]));
                config.setMax(parseDouble(raw[8]));
                config.setMin(parseDouble(raw[9]));
                config.setMean(parseDouble(raw[10]));
                config.setStdDev(parseDouble(raw[11], Double.NaN));

                // magic?
                config.setColumnType(ColumnType.of(raw[12]));

                config.setMedian(parseDouble(raw[13]));

                config.setMissingCnt(parseLong(raw[14]));
                config.setTotalCount(parseLong(raw[15]));
                config.setMissingPercentage(parseDouble(raw[16]));

                config.setBinWeightedNeg(CommonUtils.stringToDoubleList(raw[17]));
                config.setBinWeightedPos(CommonUtils.stringToDoubleList(raw[18]));
                config.getColumnStats().setWoe(parseDouble(raw[19]));
                config.getColumnStats().setWeightedWoe(parseDouble(raw[20]));
                config.getColumnStats().setWeightedKs(parseDouble(raw[21]));
                config.getColumnStats().setWeightedIv(parseDouble(raw[22]));
                config.getColumnBinning().setBinCountWoe(CommonUtils.stringToDoubleList(raw[23]));
                config.getColumnBinning().setBinWeightedWoe(CommonUtils.stringToDoubleList(raw[24]));
                // TODO magic code?
                if(raw.length >= 26) {
                    config.getColumnStats().setSkewness(parseDouble(raw[25]));
                }
                if(raw.length >= 27) {
                    config.getColumnStats().setKurtosis(parseDouble(raw[26]));
                }
                if(raw.length >= 30) {
                    config.getColumnStats().setValidNumCount(parseLong(raw[29]));
                }
                if(raw.length >= 31) {
                    config.getColumnStats().setDistinctCount(parseLong(raw[30]));
                }
                if(raw.length >= 32) {
                    if(raw[31] != null) {
                        List<String> sampleValues = Arrays.asList(Base64Utils.base64Decode(raw[31]).split(","));
                        config.setSampleValues(sampleValues);
                    }
                }
                if(raw.length >= 33) {
                    config.getColumnStats().set25th(parseDouble(raw[32]));
                }
                if(raw.length >= 34) {
                    config.getColumnStats().set75th(parseDouble(raw[33]));
                }
            } catch (Exception e) {
                log.error(String.format("Fail to process following column : %s name: %s error: %s", columnNum,
                        this.columnConfigList.get(corrColumnNum).getColumnName(), e.getMessage()), e);
                continue;
            }
        }
    }

    private static double parseDouble(String str) {
        return parseDouble(str, 0d);
    }

    private static double parseDouble(String str, double dVal) {
        try {
            return Double.parseDouble(str);
        } catch (Exception e) {
            return dVal;
        }
    }

    private static long parseLong(String str) {
        return parseLong(str, 0L);
    }

    private static long parseLong(String str, long lVal) {
        try {
            return Long.parseLong(str);
        } catch (Exception e) {
            return lVal;
        }
    }

    /**
     * Calculate the PSI
     * 
     * @throws IOException
     *             in scanners read exception
     */
    public void runPSI() throws IOException {
        log.info("Run PSI to use {} to compute the PSI ", modelConfig.getPsiColumnName());
        ColumnConfig columnConfig = CommonUtils.findColumnConfigByName(columnConfigList,
                modelConfig.getPsiColumnName());

        if(columnConfig == null || isBadPSIColumn(columnConfig.getColumnStats().getDistinctCount())) {
            log.error(
                    "Unable to use the PSI column {} specify in ModelConfig to compute PSI\n"
                            + "the distinct count {} should be [2, 1000]",
                    columnConfig != null ? columnConfig.getColumnName() : "unknown",
                    columnConfig != null
                            ? (columnConfig.getColumnStats().getDistinctCount() == null ? "null"
                                    : columnConfig.getColumnStats().getDistinctCount())
                            : "null");
            return;
        }

        log.info("Start to use {} to compute the PSI ", columnConfig.getColumnName());

        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("delimiter", CommonUtils.escapePigString(modelConfig.getDataSetDelimiter()));
        paramsMap.put("PSIColumn", modelConfig.getPsiColumnName().trim());
        paramsMap.put("column_parallel", Integer.toString(columnConfigList.size() / 10));
        paramsMap.put("value_index", "2");
        paramsMap.put(CommonConstants.MTL_INDEX, this.getMtlIndex() + "");

        PigExecutor.getExecutor().submitJob(modelConfig, pathFinder.getScriptPath("scripts/PSI.pig"), paramsMap);

        String psiPath;
        if(this.modelConfig.isMultiTask()) {
            psiPath = pathFinder.getPSIInfoPath(this.getMtlIndex());
        } else {
            psiPath = pathFinder.getPSIInfoPath();
        }

        List<Scanner> scanners = ShifuFileUtils.getDataScanners(psiPath, modelConfig.getDataSet().getSource());
        if(CollectionUtils.isEmpty(scanners)) {
            log.info("The PSI got failure during the computation");
            return;
        }

        String delimiter = Environment.getProperty(Constants.SHIFU_OUTPUT_DATA_DELIMITER, Constants.DEFAULT_DELIMITER);
        Splitter splitter = Splitter.on(delimiter).trimResults();

        List<String> unitStats = new ArrayList<String>(this.columnConfigList.size());
        for(Scanner scanner: scanners) {
            while(scanner.hasNext()) {
                String[] output = Lists.newArrayList(splitter.split(scanner.nextLine())).toArray(new String[0]);
                try {
                    int columnNum = Integer.parseInt(output[0]);
                    ColumnConfig config = this.columnConfigList.get(columnNum);
                    config.setPSI(Double.parseDouble(output[1]));
                    unitStats.add(output[0] + "|" + output[2] // PSI std
                            + "|" + output[3] // cosine
                            + "|" + output[4] // cosine std
                            + "|" + output[5]);
                } catch (Exception e) {
                    log.error("error in parsing", e);
                }
            }
            // close scanner
            IOUtils.closeQuietly(scanner);
        }

        // write unit stat into a temporary file
        ShifuFileUtils.createDirIfNotExists(new SourceFile(Constants.TMP, RawSourceData.SourceType.LOCAL));
        String ccUnitStatsFile;
        if(modelConfig.isMultiTask()) {
            ccUnitStatsFile = this.pathFinder.getColumnConfigUnitStatsPath(this.getMtlIndex());
        } else {
            ccUnitStatsFile = this.pathFinder.getColumnConfigUnitStatsPath();
        }
        ShifuFileUtils.writeLines(unitStats, ccUnitStatsFile, RawSourceData.SourceType.LOCAL);

        log.info("The Unit Stats is stored in - {}.", ccUnitStatsFile);
        log.info("Run PSI - done.");
    }

    private boolean isBadPSIColumn(Long distinctCount) {
        return (distinctCount == null || distinctCount < MINIMUM_DISTINCT_CNT || distinctCount > MAXIMUM_DISTINCT_CNT);
    }

}
