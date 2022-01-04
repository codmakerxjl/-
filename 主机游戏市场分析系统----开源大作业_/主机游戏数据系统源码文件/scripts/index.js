function excelToECharts(obj) {
    excelToData(obj);
}
// 读取Excel转换为json
function excelToData(obj) {
    let inputId = obj.id;
    let files = obj.files;
    if (files.length) {
        let reader = new FileReader();
        let file = files[0];
        // 看下文件格式是否为xls或xlsx
        let fullName = file.name;   
        let filename = fullName.substring(0, fullName.lastIndexOf("."));    // 文件名
        let fixName = fullName.substring(fullName.lastIndexOf("."), fullName.length);   // 后缀名
        // 处理excel
        if (fixName == ".xls" || fixName == ".xlsx") {
            reader.onload = function (ev) {
                let data = ev.target.result;
                // 获取excel
                let excel = XLSX.read(data, {type: 'binary'});
                // 获取第一个标签页名
                let sheetName = excel.SheetNames[0];
                // 获取第一个标签页的内容
                let sheet = excel.Sheets[sheetName];
                // 转换为JSON
                let sheetJson = XLSX.utils.sheet_to_json(sheet);

                // 转换成json后,转换线图格式
                if (inputId == 'inputLine') {
                    // 线图
                    getLineChartFromJson(sheetJson, filename);
                } else if (inputId == 'inputPie') {
                    // 饼图
                    getPieChartFromJson(sheetJson, filename);
                }
            }
        } else {
            alert("只支持excel")
        }
        reader.readAsBinaryString(file);
    }
}

// 获取列名，返回列名的数组
function getColName(sheetJson) {
    // 遍历json的第一行从而得到key
    let keys = [];
    for (let key in sheetJson[0]) {
        keys.push(key)
    }
    return keys;
}

// 数据封装及显示
function getLineChartFromJson(sheetJson, filename) {

    // 如果有结果，处理结果
    if (sheetJson.length) {
        // 获取所有列名
        let keys = getColName(sheetJson);

        // 处理一下作为x轴的列名和数据
        let xZhou = {};
        xZhou.name = keys.splice(0, 1);
        let xDatas = [];
        for (let i in sheetJson) {
            xDatas.push(sheetJson[i][xZhou.name]);
        }
        xZhou.data = xDatas;

        // 主体数据
        let datas = [];
        for (let i in keys) {
            let one = {};       // 一组
            one.name = keys[i]; 
            one.type = 'line';  // 图表类型
            one.smooth = true;  // 平滑的线
            let point = [];     // 记录这一组的所有点
            for (let idx in sheetJson) {
                // 把这组的点push到数组中
                point.push(sheetJson[idx][one.name]);
            }
            one.data = point;
            // 把这组数据添加到主体数据中
            datas.push(one)
        }

        // 调用展现的方法
        dataToLineChart(filename, keys, xZhou, datas);

    }
}


// 数据可视化
function dataToLineChart(title, keys, xZhou, datas) {
    document.getElementById('ECharts_main').removeAttribute('_echarts_instance_');
    // 基于准备好的dom，初始化echarts实例
    var myChart = echarts.init(document.getElementById('ECharts_main'));

    // 指定图表的配置项和数据
    var option = {
        title: {
            text: title,
            x: 'center',
            y: 'bottom'
        },
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data: keys,
            orient: 'vertical',
            x: 'right',
            y: 'center'
        },
        xAxis: xZhou,
        yAxis: {},
        series: datas,
        toolbox: {
            show: true,
            left: 'right',
            feature: {
                dataView: {},
                magicType: {
                    type: ['line', 'bar', 'stack', 'tiled']
                },
                saveAsImage: {}
            }
        }
    };
    // 显示图表。
    myChart.setOption(option);
}

// 饼图数据封装及显示
function getPieChartFromJson(sheetJson, filename) {
    let keys = getColName(sheetJson);
    let items = [];
    for (let i in sheetJson) {
        items.push(sheetJson[i][keys[0]]);
    }
    // 获取数据
    let sheetData = [];
    for (let i in sheetJson) {
        sheetData.push({'name': sheetJson[i][keys[0]], 'value': sheetJson[i][keys[1]]});
    }
    // 构造series要的数据
    let datas = {};
    datas.name = keys[0];         
    datas.type = 'pie';  
    datas.radius = '50%';      
    datas.center = ['45%', '50%']; 
    datas.data = sheetData;     
    datas.itemStyle = {  
        emphasis: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
        }
    };
    dataToPieChart(filename, items, datas);
}

// 饼图数据展现
function dataToPieChart(title, items, datas) {
    
    document.getElementById('ECharts_main').innerHTML = "";
    
    document.getElementById('ECharts_main').removeAttribute('_echarts_instance_');
    // 基于准备好的dom，初始化echarts实例
    let myChart = echarts.init(document.getElementById('ECharts_main'));

    // 指定图表的配置项和数据
    let option = {
        title: {
            text: title,
            x: 'center',
            y: 'bottom'
        },
        tooltip: {
            trigger: 'item',
            formatter: "{a} <br/>{b} : {c} ({d}%)"
        },
        legend: {
            type: 'scroll',
            orient: 'vertical',
            right: 10,
            top: 30,
            bottom: 20,
            data: items,
        },
        series: datas,
        toolbox: {
            show: true,
            left: 'right',
            feature: {
                dataView: {},
                magicType: {},
                saveAsImage: {}
            }
        }
    };

    // 使用刚指定的配置项和数据显示图表。
    myChart.setOption(option);
}
