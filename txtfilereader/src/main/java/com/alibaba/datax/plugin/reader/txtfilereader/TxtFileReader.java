package com.alibaba.datax.plugin.reader.txtfilereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.google.common.collect.Sets;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public class TxtFileReader extends Reader {
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration originConfig = null;

		private List<String> path = null;

		private List<String> sourceFiles;

		private Map<String, Pattern> pattern;

		private Map<String, Boolean> isRegexPath;

		@Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
			this.pattern = new HashMap<String, Pattern>();
			this.isRegexPath = new HashMap<String, Boolean>();
			this.validateParameter();
		}

		private void validateParameter() {
			// Compatible with the old version, path is a string before
			String pathInString = this.originConfig.getNecessaryValue(Key.PATH,
					TxtFileReaderErrorCode.REQUIRED_VALUE);
			if (StringUtils.isBlank(pathInString)) {
				throw DataXException.asDataXException(
						TxtFileReaderErrorCode.REQUIRED_VALUE,
						"?????????????????????????????????????????????");
			}
			if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
				path = new ArrayList<String>();
				path.add(pathInString);
			} else {
				path = this.originConfig.getList(Key.PATH, String.class);
				if (null == path || path.size() == 0) {
					throw DataXException.asDataXException(
							TxtFileReaderErrorCode.REQUIRED_VALUE,
							"?????????????????????????????????????????????");
				}
			}

			String encoding = this.originConfig
					.getString(
							com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
							com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
			if (StringUtils.isBlank(encoding)) {
                this.originConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
                                com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
			} else {
				try {
					encoding = encoding.trim();
					this.originConfig
							.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
									encoding);
					Charsets.toCharset(encoding);
				} catch (UnsupportedCharsetException uce) {
					throw DataXException.asDataXException(
							TxtFileReaderErrorCode.ILLEGAL_VALUE,
							String.format("????????????????????????????????? : [%s]", encoding), uce);
				} catch (Exception e) {
					throw DataXException.asDataXException(
							TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
							String.format("??????????????????, ???????????????: %s", e.getMessage()),
							e);
				}
			}

			// column: 1. index type 2.value type 3.when type is Date, may have
			// format
			List<Configuration> columns = this.originConfig
					.getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
			// handle ["*"]
			if (null != columns && 1 == columns.size()) {
				String columnsInStr = columns.get(0).toString();
				if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
					this.originConfig
							.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN,
									null);
					columns = null;
				}
			}

			if (null != columns && columns.size() != 0) {
				for (Configuration eachColumnConf : columns) {
					eachColumnConf
							.getNecessaryValue(
									com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE,
									TxtFileReaderErrorCode.REQUIRED_VALUE);
					Integer columnIndex = eachColumnConf
							.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
					String columnValue = eachColumnConf
							.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

					if (null == columnIndex && null == columnValue) {
						throw DataXException.asDataXException(
								TxtFileReaderErrorCode.NO_INDEX_VALUE,
								"??????????????????type, ????????????????????? index ??? value");
					}

					if (null != columnIndex && null != columnValue) {
						throw DataXException.asDataXException(
								TxtFileReaderErrorCode.MIXED_INDEX_VALUE,
								"??????????????????index, value, ???????????????????????????????????????");
					}
					if (null != columnIndex && columnIndex < 0) {
						throw DataXException.asDataXException(
								TxtFileReaderErrorCode.ILLEGAL_VALUE, String
										.format("index??????????????????0, ????????????index???[%s]",
												columnIndex));
					}
				}
			}

			// only support compress types
			String compress = this.originConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS);
			if (StringUtils.isBlank(compress)) {
				this.originConfig
						.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS,
								null);
			} else {
				Set<String> supportedCompress = Sets
						.newHashSet("gzip", "bzip2", "zip");
				compress = compress.toLowerCase().trim();
				if (!supportedCompress.contains(compress)) {
					throw DataXException
							.asDataXException(
									TxtFileReaderErrorCode.ILLEGAL_VALUE,
									String.format(
											"????????? gzip, bzip2, zip ?????????????????? , ???????????????????????????????????????: [%s]",
											compress));
				}
				this.originConfig
						.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS,
								compress);
			}

			String delimiterInStr = this.originConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER);
			// warn: if have, length must be one
			if (null != delimiterInStr && 1 != delimiterInStr.length()) {
				throw DataXException.asDataXException(
						UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
						String.format("???????????????????????????, ????????????????????? : [%s]",
								delimiterInStr));
			}

		}

		@Override
		public void prepare() {
			LOG.debug("prepare() begin...");
			// warn:make sure this regex string
			// warn:no need trim
			for (String eachPath : this.path) {
				String regexString = eachPath.replace("*", ".*").replace("?",
						".?");
				Pattern patt = Pattern.compile(regexString);
				this.pattern.put(eachPath, patt);
				this.sourceFiles = this.buildSourceTargets();
			}

			LOG.info(String.format("??????????????????????????????: [%s]", this.sourceFiles.size()));
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

		// warn: ???????????????????????????????????????????????????=>??????????????????????????????
		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.debug("split() begin...");
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

			// warn:??????slice????????????????????????,
			// int splitNumber = adviceNumber;
			int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.EMPTY_DIR_EXCEPTION, String
                                .format("??????????????????????????????,????????????????????????path: %s",
                                        this.originConfig.getString(Key.PATH)));
            }

			List<List<String>> splitedSourceFiles = this.splitSourceFiles(
					this.sourceFiles, splitNumber);
			for (List<String> files : splitedSourceFiles) {
				Configuration splitedConfig = this.originConfig.clone();
				splitedConfig.set(Constant.SOURCE_FILES, files);
				readerSplitConfigs.add(splitedConfig);
			}
			LOG.debug("split() ok and end...");
			return readerSplitConfigs;
		}

		// validate the path, path must be a absolute path
		private List<String> buildSourceTargets() {
			// for eath path
			Set<String> toBeReadFiles = new HashSet<String>();
			for (String eachPath : this.path) {
				int endMark;
				for (endMark = 0; endMark < eachPath.length(); endMark++) {
					if ('*' != eachPath.charAt(endMark)
							&& '?' != eachPath.charAt(endMark)) {
						continue;
					} else {
						this.isRegexPath.put(eachPath, true);
						break;
					}
				}

				String parentDirectory;
				if (BooleanUtils.isTrue(this.isRegexPath.get(eachPath))) {
					int lastDirSeparator = eachPath.substring(0, endMark)
							.lastIndexOf(IOUtils.DIR_SEPARATOR);
					parentDirectory = eachPath.substring(0,
							lastDirSeparator + 1);
				} else {
					this.isRegexPath.put(eachPath, false);
					parentDirectory = eachPath;
				}
				this.buildSourceTargetsEathPath(eachPath, parentDirectory,
						toBeReadFiles);
			}
			return Arrays.asList(toBeReadFiles.toArray(new String[0]));
		}

		private void buildSourceTargetsEathPath(String regexPath,
				String parentDirectory, Set<String> toBeReadFiles) {
			// ????????????????????????????????????????????????
			try {
				File dir = new File(parentDirectory);
				boolean isExists = dir.exists();
				if (!isExists) {
					String message = String.format("??????????????????????????? : [%s]",
							parentDirectory);
					LOG.error(message);
					throw DataXException.asDataXException(
							TxtFileReaderErrorCode.FILE_NOT_EXISTS, message);
				}
			} catch (SecurityException se) {
				String message = String.format("??????????????????????????? : [%s]",
						parentDirectory);
				LOG.error(message);
				throw DataXException.asDataXException(
						TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
			}

			directoryRover(regexPath, parentDirectory, toBeReadFiles);
		}

		private void directoryRover(String regexPath, String parentDirectory,
				Set<String> toBeReadFiles) {
			File directory = new File(parentDirectory);
			// is a normal file
			if (!directory.isDirectory()) {
				if (this.isTargetFile(regexPath, directory.getAbsolutePath())) {
					toBeReadFiles.add(parentDirectory);
					LOG.info(String.format(
							"add file [%s] as a candidate to be read.",
							parentDirectory));

				}
			} else {
				// ?????????
				try {
					// warn:???????????????????????????,listFiles ??????null??????????????????SecurityException
					File[] files = directory.listFiles();
					if (null != files) {
						for (File subFileNames : files) {
							directoryRover(regexPath,
									subFileNames.getAbsolutePath(),
									toBeReadFiles);
						}
					} else {
						// warn: ???????????????????????????????????????throw DataXException
						String message = String.format("??????????????????????????? : [%s]",
								directory);
						LOG.error(message);
						throw DataXException.asDataXException(
								TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH,
								message);
					}

				} catch (SecurityException e) {
					String message = String.format("??????????????????????????? : [%s]",
							directory);
					LOG.error(message);
					throw DataXException.asDataXException(
							TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH,
							message, e);
				}
			}
		}

		// ????????????
		private boolean isTargetFile(String regexPath, String absoluteFilePath) {
			if (this.isRegexPath.get(regexPath)) {
				return this.pattern.get(regexPath).matcher(absoluteFilePath)
						.matches();
			} else {
				return true;
			}

		}

		private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
				int adviceNumber) {
			List<List<T>> splitedList = new ArrayList<List<T>>();
			int averageLength = sourceList.size() / adviceNumber;
			averageLength = averageLength == 0 ? 1 : averageLength;

			for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
				end = begin + averageLength;
				if (end > sourceList.size()) {
					end = sourceList.size();
				}
				splitedList.add(sourceList.subList(begin, end));
			}
			return splitedList;
		}

	}

	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration readerSliceConfig;
		private List<String> sourceFiles;

		@Override
		public void init() {
			this.readerSliceConfig = this.getPluginJobConf();
			this.sourceFiles = this.readerSliceConfig.getList(
					Constant.SOURCE_FILES, String.class);
		}

		@Override
		public void prepare() {

		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {

		}

		@Override
		public void startRead(List<RecordSender> recordSenders) {
			LOG.debug("start read source files...");
			for (String fileName : this.sourceFiles) {
				LOG.info(String.format("reading file : [%s]", fileName));
				InputStream inputStream;
				try {
					inputStream = new FileInputStream(fileName);
					UnstructuredStorageReaderUtil.readFromStream(inputStream,
							fileName, this.readerSliceConfig, recordSenders,
							this.getTaskPluginCollector());
					for(RecordSender recordSender:recordSenders){
						recordSender.flush();
					}
				} catch (FileNotFoundException e) {
					// warn: sock ????????????read,??????????????????????????????,????????????????????????
					String message = String
							.format("??????????????????????????? : [%s]", fileName);
					LOG.error(message);
					throw DataXException.asDataXException(
							TxtFileReaderErrorCode.OPEN_FILE_ERROR, message);
				}
			}
			LOG.debug("end read source files...");
		}

	}
}
