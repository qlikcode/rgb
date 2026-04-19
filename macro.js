
var fso 						= new ActiveXObject("Scripting.FileSystemObject");
var wshShell 					= new ActiveXObject("WScript.Shell");
var objShell 					= new ActiveXObject("Shell.Application");
var binaryStream 				= new ActiveXObject("ADODB.Stream");

var document					= ActiveDocument;
var gitPath						= ActiveDocument.Evaluate("=$(v(get,GitPath))");
var qvsPath						= gitPath +'/QVS/# '+ GetParam('Name') +'.qvs';
var txtPath						= gitPath +'/TXT/QV/';


String.prototype.trim 			= function() { return this.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, '') }
String.prototype.replaceAll 	= function(str1, str2, ignore) { return this.replace(new RegExp(str1.replace(/([\/\,\!\\\^\$\{\}\[\]\(\)\.\*\+\?\|\<\>\-\&])/g,"\\$&"),(ignore?"gi":"g")),(typeof(str2)=="string")?str2.replace(/\$/g,"$$$$"):str2) } 
Array.prototype.contains 		= function(obj) { var i = this.length; while (i--) { if (this[i] === obj) { return true; } } return false; }

function sleep(sec){			//Задержка работы макроса
    var unixtime_ms = new Date().getTime(); while(new Date().getTime() < unixtime_ms + (sec * 1000)) {} 
}

function readBinary(path, type){
	binaryStream.Type 			= type; 	// 1 - file, 2 - text
	if( type == 2 ) { binaryStream.Charset 	= "utf-8" }
	binaryStream.Open();
	binaryStream.LoadFromFile( path );
	if( type == 2 ) {  //qvlib.msgbox( path );
		stream = binaryStream.ReadText();
	} else {
		stream = binaryStream.Read();
	} 
	binaryStream.Close();
	return stream
}

function writeBinary(path, type, stream){ 	
	binaryStream.Type 		= type; 						// 1 - file, 2 - text
	if( type == 2 ) { binaryStream.Charset 	= "utf-8" } 	// Кодировка UTF-8
	binaryStream.Open(); 
	if( type == 2 ) { 
		binaryStream.WriteText(stream);
	} else {
		binaryStream.Write(stream);
	}
	binaryStream.SaveToFile (path, 2); 
	binaryStream.Close();
}

function writeText(path, text){
	var oFile 	= (fso.FileExists(vPath)) ? fso.OpenTextFile(vPath, 2, true) : fso.CreateTextFile(vPath)
	oFile.Write (vText) 
	oFile.Close() 	
}

//eval(readBinary("//172.16.245.8/QlikView/TXT/Macro.txt", 2));



function getScript(){
	getScript =  document.GetProperties().Script
	return getScript
}

function Restore(){ // qvlib.msgbox(1);
	Application.MsgBox("Восстановление", 0 )
	popup 	= wshShell.popup("Восстановить по названию файла?", 3, '', 1); 
	if(popup == 1){
		name = GetParam('Name');
		var properties 		= document.GetProperties();
		properties.Script 	= readBinary(qvsPath, 2);
		document.SetProperties( properties );

		var varArr = 'v,vEnums,vMetas,vInput,vParams,vScript'.split(','); //GetParam('KeepVars').split(','); 
		for( i = 0 ; i < varArr.length; i++){ 
			path = txtPath + name +'.'+ varArr[i] +'.txt'; // qvlib.msgbox(path)
			if( fso.FileExists(path) && fso.GetFile(path).Size > 0 ){ 
				text = readBinary( path, 2 ); 
				document.GetApplication().WaitForIdle();
				document.Variables( varArr[i] ).SetContent( text, true );
			} else {
				qvlib.msgbox('Путь к переменной '+ varArr[i] +' не найден')
			}
		}
		SetParam('Action', 'None')
		
		SetParam('Params', '0' )
		SetParam('GitPath', '//172.16.245.8/QlikView/GIT/RGB' )
		SetParam('LoadMode', 'External' )
		SetParam('EditMode', '0' )
		
		document.GetApplication().WaitForIdle();
		document.Reload()
		SetAction()
		Application.MsgBox("Готово!");
	}
}

function SetScript(vScript){
	var Properties 		= ActiveDocument.GetProperties();
	Properties.Script 	= vScript;
	ActiveDocument.SetProperties(Properties);	
}

function ReplaceInScript(vOld, vNew){
    var Properties = ActiveDocument.GetProperties(); 
 	Properties.Script = Properties.Script.split(vOld).join(vNew);
    ActiveDocument.SetProperties(Properties);
}

function LoadMetas(){ 
	Application.MsgBox("Загрузка метаданных", 0 )
	vPopup = wshShell.popup("Загрузить из excel?", 0, '', 3); 
	if(vPopup == 2){ return }
	if(vPopup == 6){ 
		var objFile = objShell.BrowseForFolder(0, "Выберите файл", "&H4000", "\\\\172.16.245.8\\qlikview\\XLS\\Metas")
	    if(objFile==null ){  return } //qvlib.MsgBox("Файл не выбран");
		if(!fso.FileExists(objFile.self.path)){ qvlib.MsgBox("Файл не найден"); return }
	} 
	sql 	= vPopup == 6 ? '' : ' sql';
	txt 	= vPopup == 6 ? ' из excel ' + objFile.self.path : ' из sql';
	load 	= 'Full load';
	icon 	= 1
	
	if ( document.evaluate( "FieldIndex( '$Table', 'Metas')" ) > 0 ) {
		msg3 = wshShell.popup("Сохранить настройки?", 0, '', 3); 
		if(msg3 == 2){ return }
		txt += msg3 == 6 ? ' с сохранением настроек' : ' с потерей настроек';
		load = msg3 == 6 ? 'Keep load' : 'Full load';
		icon += msg3 == 6 ? 0 : 48;
	}
		
  	vPopup = wshShell.popup("Будут загружены метаданые " + txt +" Продолжить?", 3, '', icon); //Будут загружены метаданые
  	//vPopup = wshShell.Popup(f"Будут загружены метаданые {txt} Продолжить?", 3, "", icon)	
	
	if(vPopup == 1){ // Если, да, то создаем переменную-маркер для указания в загрузке, что нужен только сгенерированный скрипт
		SetParam('Action', load + sql); 
		if (sql == '') { SetParam('MetaPath', objFile.self.path) } else { getMeta() } 
		ActiveDocument.Reload()
		SaveEnums()
		SaveMetas()
		SetParam('Action', 'None');
		ActiveDocument.Reload()
	} 
	SetAction()
}

function LoadMetas_(){
	Application.MsgBox("Загрузка метаданных из файла", 0 )
    var objFile = objShell.BrowseForFolder(0, "Выберите файл", "&H4000", "\\\\172.16.245.8\\qlikview\\XLS\\Metas") 
    if(objFile==null ){ qvlib.MsgBox("Файл не выбран"); return } 
	if(!fso.FileExists(objFile.self.path)){ qvlib.MsgBox("Файл не найден"); return }
	SetParam('Action', 'Keep load'); var vText = ' с сохранением настроек.';
  	vPopup = wshShell.popup("Сохранить настройки запросов?", 0, '', 4);  
	if(vPopup == 7){ SetParam('Action', 'Full load'); vText = ' БЕЗ СОХРАНЕНИЯ НАСТРОЕК.'; }	
  	vPopup = wshShell.popup("Будут загружены метаданые из файла "+ objFile.self.path + vText +" Продолжить?", 3, '', 1);  
	if(vPopup == 1){ // Если, да, то создаем переменную-маркер для указания в загрузке, что нужен только сгенерированный скрипт
		SetParam('MetaPath', objFile.self.path)
		ActiveDocument.Reload()
		SaveEnums()
		SaveMetas()
		SetParam('Action', 'None');
		ActiveDocument.Reload()
	} // else { SetAction() }	
	SetAction()
}

function EditMetas(){
	SetParam('Action', 'Edit')
	ActiveDocument.Reload()
//	ActiveDocument.Fields("Select1").ResetInputFieldValues(0) 
} 

function SaveMetas(){
	ActiveDocument.CreateVariable("vMetas")
	ActiveDocument.Variables("vMetas").SetContent(ActiveDocument.Evaluate("=Concat({1< [$Table] = {'Metas'}, [$Field] -= {'Default'} >} '['& [$Field] &']',  Chr(9), [$FieldNo]) & Chr(13) & Concat({1}  $(=Concat({1< [$Table] = {'Metas'}, [$Field] -= {'Default'} >} '['& [$Field] &']', ' & Chr(9) & ', [$FieldNo])), Chr(13), НомерСтроки) "), true)
//	SetParam('Action', 'None')
//	ActiveDocument.Reload()
	SetAction()
}

function SaveEnums(){
	ActiveDocument.CreateVariable("vEnums")
 	ActiveDocument.Variables("vEnums").SetContent(ActiveDocument.Evaluate("=Concat({1} Enums.Перечисление &Chr(9)& Enums.Значение &Chr(9)& Enums.Синоним &Chr(9)& Enums.Порядок, Chr(13)) "), true)
}

function OnOpen(){  
	ActiveDocument.RecallDocBookmark("Default")
	SetParam('Action', 'Default');
	SetParam('Period', 'Default');
	SetParam('Select', 'Default');
	ActiveDocument.Fields("$Field").Select( "(ИмяТаблицыQV|ИмяПоляQV|Синоним|ИмяХранения|Select1)" );
	SetInput();
	GetRoutines();
	CheckVariables();
	// ActiveDocument.RemoveUserBookmark "Default"
	// ActiveDocument.CreateUserBookmark "Default"
	// SetParam('InputType', 'Single')
}

function onOpenApp() {
	CheckVariables();
	ActiveDocument.Variables("n").SetContent(1, true);

    // Устанавливаем выборку по текущим значениям для года, месяца и дня
    document.fields("Год").select(document.evaluate("=Year(Today())"));
    document.fields("Месяц").select(document.evaluate("=Month(Today())"));
    document.fields("Число").select(document.evaluate("=Day(Today())"));

    document.fields("D1").select(document.evaluate("=FirstSortedValue({<D1>} D1, Aggr(RowNo(TOTAL), D1))"));
    document.fields("M1").select(document.evaluate("=FirstSortedValue({<M1>} M1, Aggr(RowNo(TOTAL), M1))"));
}

function OnPostReload(){  //qvlib.MsgBox(1);
	SetParam('KeepVars', 'v,vInput,vParams,vMetas,vEnums,vScript' )
//	SetInput()
	GetRoutines();
	CheckVariables();
	// SaveScript();
	// SetTriggers();
	// WriteTxt('//172.16.245.8/QlikView/TXT/QV/'+ GetParam('Name') +'.Script.txt', ActiveDocument.GetProperties().Script)
	// ReplaceInScript('$(vQvdFolder)', '$(vQvdPath)\\')
	// vQvdPath = ActiveDocument.Variables("vQvdFolder").GetContent().String
	// SetParam('SQL', 'MS')
	// DelParam('Variables')
	// ActiveDocument.Variables("vEditTable").SetContent(0, true);
	ActiveDocument.GetApplication().WaitForIdle();
}

function GetRoutines(){  //qvlib.MsgBox(vSubList);
	var vString='', vValue='';
	var script = ActiveDocument.Evaluate("=$(v(get,LoadMode))") == 'External' ? readBinary(qvsPath, 2) : ActiveDocument.GetProperties().Script
//	var vSubArray = ActiveDocument.GetProperties().Script.split("SUB "); 
	
	var vSubArray = script.split("SUB "); 
	for(var i = 1; i < vSubArray.length; i++){ 
		vValue = vSubArray[i].split(" ")[0]; 
		vString += (vSubArray[i-1].trim().slice(-3).toUpperCase() != "END" && vSubArray[i-1].trim().slice(-2) != "//" && vValue.split(";")[0].trim().length > 0 && vValue.substring(0, 1) != '#' )? vValue.split(";")[0].split("(")[0] + ";" : ""; // && !vValue.trim().match("//*")¶
	} 
	vString = vString.trim().slice(0, vString.length-1).replace("undefined", "")
	vString += vString.split(';').contains("Copy") ? "" : ";Copy"
	//qvlib.msgbox(vString)
	//ActiveDocument.Variables("vScript").SetContent(vString, true);
	SetParam('AllRoutines', vString)
}

function CheckVariables(){ //qvlib.msgbox(1)
	var vars = document.GetVariableDescriptions()
	var dropVars = GetParam('DropVars');
	var keepVars = GetParam('KeepVars');
	for( i = 0 ; i < vars.Count; i++){
		v = vars.Item(i)
		if ( !v.IsConfig && !v.IsReserved){ 
			if (!keepVars.split(',').contains(v.Name)  ) { //|| v.RawValue.length == 0
				if( !dropVars.split(',').contains(v.Name) ){
					dropVars += ',' + v.Name; 
				}
				if (v.Name.length > 0) { document.RemoveVariable(v.Name) }
			} else if ( keepVars.split(',').contains(v.Name) ) { 
				// writeBinary(txtPath + GetParam('Name') +'.'+ v.Name +'.txt', 2, v.RawValue)
			}				
		}
    }
	SetParam('DropVars', dropVars); 
	ActiveDocument.GetApplication().WaitForIdle();
} 


function ExportVariablesToExcel(){
	var doc = ActiveDocument
       
	var objExcel = new ActiveXObject('Excel.Application');
	var objWorkbook = objExcel.Workbooks.Add
    var objSheet = objWorkbook.Sheets.Add
    
    objSheet.Name = "Variables"   
	objSheet.Cells(1, 1).Value = "Variable"
 	objSheet.Cells(1, 2).Value = "Expression"
 	objSheet.Cells(1, 3).Value = "Comment"
       
	var vars = ActiveDocument.GetVariableDescriptions()
	r = 2
	for( i = 0 ; i < vars.Count; i++){
		 v = vars.Item(i)
 			if ( !v.IsConfig && !v.IsReserved){
				objSheet.Cells(r, 1).Value = v.Name
                if( v.RawValue.substring(0, 1) == "=" ){			
					objSheet.Cells(r, 2).Value = "'" & v.RawValue
				}else{
					objSheet.Cells(r, 2).Value = v.RawValue
				}
 				objSheet.Cells(r, 3).Value = ActiveDocument.Variables(v.Name).GetComment()
			r = r + 1
			}
       }
	objExcel.Visible = true
} 

/* function CommonScript(){
	ActiveDocument.CreateVariable("vCommonScript")
	ActiveDocument.Variables("vCommonScript").SetContent(ActiveDocument.GetProperties().Script, true)
} */

function SetInput(){ 
	if( GetParam('Action') == 'Queries' ) 			{ SetParam('Queries', 	ActiveDocument.Variables("vInput").GetContent().String ); 
														ActiveDocument.Fields('ИмяТаблицыQV').Select( ActiveDocument.Variables("vInput").GetContent().String ) }
	else if ( GetParam('Action') == 'Routines' )	{ SetParam('Routines', 	ActiveDocument.Variables("vInput").GetContent().String ) }
}


//personName = qvlib.InputBox("What is you name?")

function SetAction	(){ setParams('Action'	) }
function SetPeriod	(){ setParams('Period'	) }
function SetSelect	(){ setParams('Select'	) }
function SetQueries	(){ setParams('Queries'	) }
function SetRoutines(){ setParams('Routines') }

function SetQvdPath	(){ setParams('QvdPath'	, 'Qvd path') }
function SetServer	(){ setParams('Server'	, 'Server'	) }
function SetDatabase(){ setParams('Database', 'Database') }
function SetLogin	(){ setParams('Login'	, 'Login'	) }
function SetPassword(){ setParams('Password', 'Password') }

function setParams(param, text){  
	if (['QvdPath','Server','Database','Login','Password'].contains(param) ) {
		value = qvlib.InputBox( text, GetParam(param) ); //"Введите " +
		SetParam(param, value.trim().length == 0 ? "undefined" : value)
	} else {
		var field = param == 'Queries' ? 'ИмяТаблицыQV' : param == 'Routines' ? '$Routine' : "$" + param;
		var selected =  document.Evaluate("=Concat({< ["+ field +"] = {'=GetSelectedCount(["+ field +"]) > 0'} >} Distinct ["+ field +"], ';')");
		SetParam(param, selected); //qvlib.MsgBox(1)
		// if (param == 'Action' && ( selected == 'Queries' || selected == 'Routines' )) {  
			// ActiveDocument.Variables("vInput").SetContent( selected == 'Queries' ? GetParam('Queries') : GetParam('Routines') , true)
			document.CreateVariable("vInput")
			document.Variables("vInput").SetContent( GetParam('Action') == 'Queries' ? GetParam('Queries') : GetParam('Routines') , true)
		// }
	}
}

function SetTriggers(){
	var vFields = ['$Action', '$Period', '$Select', '$Routine', 'ИмяТаблицыQV'];
	for( i = 0 ; i < vFields.length; i++){ //qvlib.Msgbox(vFields[i])
		RemoveTrigger(vFields[i])
		var vMacro	= vFields[i] == 'ИмяТаблицыQV' ? 'SetQueries' : vFields[i] == '$Routine' ? 'SetRoutines' : "Set" + vFields[i].replace("$", "");
		AddTrigger(vFields[i], 13, vMacro);//'SetParams')
		// AddOnChangeTrigger(vFields[i], 13, vMacro);//'SetParams')
		if( i < 3) { SetOnlyOne(vFields[i], 'Default') }
    }
} 
function SetOnlyOne(vFieldName, vValue){
	var vField 		= ActiveDocument.GetField(vFieldName) 
	var vProperties	= vField.GetProperties()	
	vField.Select( vValue )
	vProperties.OneAndOnlyOne = true
	vField.SetProperties(vProperties)
}

function RemoveTrigger(vFieldName){
	var vField 		= ActiveDocument.GetField(vFieldName) 
	var vProperties	= vField.GetProperties()	
	var vActions	= vProperties.OnSelectActionItems 
	for(var i = vActions.Count - 1; i  >= 0; i--){
		vActions.Removeat(i)
		vField.SetProperties(vProperties)
	}
}

function AddTrigger(vFieldName, vType, vParam){
	var vField 		= ActiveDocument.GetField(vFieldName) 
	var vProperties	= vField.GetProperties()	
	var vActions	= vProperties.OnSelectActionItems
	//set actions = prop.OnChangeActionItems 
	vActions.Add()
	var i 			= vProperties.OnSelectActionItems.count-1
	vActions(i).Type = vType // 13 - macro, 31 - variable
	vActions(i).Parameters.add()
	vActions(i).Parameters(0).v = vParam 
	//actions(i).Parameters.add()
	//actions(i).Parameters(1).v = "test"
	vField.SetProperties(vProperties) 
}

function AddOnChangeTrigger(vFieldName, vType, vParam){
	var vField 		= ActiveDocument.GetField(vFieldName) 
	var vProperties	= vField.GetProperties()	
	var vActions	= vProperties.OnChangeActionItems	
	//set actions = prop.OnChangeActionItems 
	vActions.Add()
	var i 			= vProperties.OnChangeActionItems.count-1
	vActions(i).Type = vType // 13 - macro, 31 - variable
	vActions(i).Parameters.add()
	vActions(i).Parameters(0).v = vParam 
	//actions(i).Parameters.add()
	//actions(i).Parameters(1).v = "test"
	vField.SetProperties(vProperties) 
}


function SetTrigger(vAction, vObjName, vObjType, vTriggerType, vParamType1, vParamValue1){
	var vField 		= ActiveDocument.GetField(vObjName) 
	var vProperties	= vField.GetProperties()	
	var vActions	= vProperties.OnChangeActionItems	
	//set actions = prop.OnChangeActionItems 
	vActions.Add()
	var i 			= vProperties.OnChangeActionItems.count-1
	vActions(i).Type = vType // 13 - macro, 31 - variable
	vActions(i).Parameters.add()
	vActions(i).Parameters(0).v = vParam 
	//actions(i).Parameters.add()
	//actions(i).Parameters(1).v = "test"
	vField.SetProperties(vProperties) 
}



/* // Тригерная процедура для фиксации изменений всех параметров
function SetParams(){  
	var vParams = ['Action', 'Period', 'Select', 'Routines', 'Queries'];
	for( i = 0 ; i < vParams.length; i++){ 
		vParam = vParams[i];
		vField = vParams[i] == 'Queries' ? 'ИмяТаблицыQV' : vParams[i] == 'Routines' ? '$Routine' : "$" + vParams[i];
		var vSelected =  ActiveDocument.Evaluate("=Concat({< ["+ vField +"] = {'=GetSelectedCount(["+ vField +"]) > 0'} >} Distinct ["+ vField +"], ';')");
		SetParam(vParam, vSelected)
    }
} */



// Присваиваем значение или добавляем параметр в vParams. Если vNewParam <> undefined, используем его, и меняем на vParam, если найдется.
function SetParam(vParam, vValue, vNewParam){ 
	// Исходная строка
	var originalString = '\n' + ActiveDocument.Variables("vParams").GetContent().String + '\n';
	// Найти позицию первого вхождения подстроки
	var startIndex = originalString.indexOf("\n" + vParam + "=") +1;  
	// Если подстрока найдена
	if (startIndex !== 0) { 
	  	// Найти индекс переноса строки после подстроки
	  	var endIndex = originalString.indexOf('\n', startIndex);
	  	// Заменить найденную подстроку на новую
	    var newString = originalString.slice(1, startIndex) + (typeof vNewParam !== 'undefined' ? vNewParam : vParam) + "=" + vValue + originalString.slice(endIndex, originalString.length -1)
	    //var newString = originalString.slice(1, originalString.length -1).replace(originalString.slice(startIndex, endIndex), vParam + "=" + vValue)
	} else {
	  // Добавить новый параметр
	  var newString = originalString.slice(1) + (typeof vNewParam !== 'undefined' ? vNewParam : vParam) + "=" + vValue; 
	}
	// Сохранить переменную
	ActiveDocument.Variables("vParams").SetContent( newString, true) 
}

// Получаем значение параметра из vParams
function GetParam(vParam){ 
	// Исходная строка
	var originalString = '\n' + ActiveDocument.Variables("vParams").GetContent().String + '\n';
	// Найти позицию первого вхождения параметра
	var startIndex = originalString.indexOf("\n" + vParam + "=") +1;
	// Если подстрока найдена
	if (startIndex !== 0) {
		// Найти позицию первого вхождения значения
		var startIndex = originalString.indexOf("=", startIndex) +1;
		// Если подстрока найдена
		var endIndex = originalString.indexOf('\n', startIndex);
		// Заменить найденную подстроку на новую
		var vValue = originalString.slice(startIndex, endIndex)//.trim()
	// Иначе undefined
	} else { var vValue = 'undefined' }
	// Возвращаем значение
	return( vValue )
}
// Удаляем параметр из vParams
function DelParam(vParam){ 
	// Исходная строка
	var originalString = '\n' + ActiveDocument.Variables("vParams").GetContent().String + '\n';
	// Найти позицию первого вхождения подстроки
	var startIndex = originalString.indexOf("\n" + vParam + "=") +1;  
	// Если подстрока найдена
	if (startIndex !== 0) { 
	  	// Найти индекс переноса строки после подстроки
	  	var endIndex = originalString.indexOf('\n', startIndex);
	  	// Добавим модификатор на случай удаления первой или последней строки
	  	var x =  startIndex == 1 ? 1 : 0 
		// Удаляем
	    var newString = originalString.slice(1, startIndex -1 + x) + originalString.slice(endIndex + x, originalString.length -1)
		// Сохранить переменную
		ActiveDocument.Variables("vParams").SetContent( newString, true) 
	}
}



// Добавление в скрипт проверки соединений
var vEnd = 1, vCnt1 = 0, vPnt1 = 0, vPnt2 = 0, vSub = '', vSub2 = '', vNch1 = '', vNch2 = '', vChrNo = 0, vText = 0, vName = 0, vCom1 = 0, vCom2 = 0, vComm = 0, vPrth = 0, vStr1 = '', vStr2 = '', vStr3 = '', vStr4 = '', vStr5 = '', vStr6 = '', vStr7 = '', vStr8 = '', vCom3 = 0, vWord = -1, qwe = 0, vChar = '', result = '', vCnt = 0, vIndx = 0, vArr, vTest, vStr5 = '', vJoin = '', vLoad = 0, vTab1 = '', vTab2 = '', vType = '', vPoint = 0, vColn = 0
function TestScript(){
	vSub = ActiveDocument.GetProperties().Script; 
	vNch1 = 'abcdefghijklmnopqrstuvwxyz@#$%^.1234567890_абвгдеёжзийклмнопрстуфхцчшщъыьэюя';
	vNch2 = '=;:,';
	
//	var X = vSub.slice(vSub.indexOf('SUB СоответствияКлиентовИЯЯКлиентов')) 
//	vSub = X.slice(0, X.indexOf('ENDSUB') + 6) 	
	vSub2 = vSub
	
	for ( i = 0; i < vSub.length; i++) {
		vChar = vSub.charAt(i)
		vChrNo = i
		vWord = i - vWord == 1 ? vWord : -1
		vStr7 = vStr3.slice(vStr3.lastIndexOf(";"))
		if( vChar == ';' && vEnd ) { vStr7 = ''; vJoin = ''; vLoad = 0; vTab1 = ''; vTab2 = '';  vType = '', vPoint = 0; vStr5 = '' }
		
			 if( vText == 1 )	{ vStr1 += vChar; if( vStr1.match(/'/g).length % 2 == 0 ) 						{ vText = 0; vComm = 0; vStr1 = ''  	} }
		else if( vChar == "'" && vComm !== 1 )  																{ vText = 1; vComm = 1; vStr1 += vChar 	}	
		else if( vName == 1 )	{ if( vSub.charAt(i).match(/^(\"|])$/) ) 										{ vName = 0; vComm = 0; 			 	} else { vStr5 = vStr5 + ( vPrth == 0  ? vChar : '' ); Func_14() } }														// Пишем текст из [] или "" в vStr5 и выкидываем его из vStr3																	
		else if( vSub.charAt(i).match(/^(\"|\[)$/) && vComm !== 1 ) 											{ vName = 1; vComm = 1; Func_11(); 	 	vStr5 = '';					vStr3 += vPrth !== 1 ? (vStr3.slice(-1) == ' ' ? '' : ' ') + '[]' : ''   } 						// Определяем [] и ""	
		else if( vCom1 == 1 )	{ if( vSub.charAt(i +1) == String.fromCharCode(13) )  							{ vCom1 = 0; vComm = 0 					} }
		else if( vSub.substring(i, i + 2) == '\/\/' && vComm !== 1 ) 											{ vCom1 = 1; vComm = 1 					} 
		else if( vCom2 == 1 )	{ if( vSub.substring(i - 1, i +1) == '\*\/' )  									{ vCom2 = 0; vComm = 0 					} }
		else if( vSub.substring(i, i + 2) == '\/\*' && vComm !== 1 ) 											{ vCom2 = 1; vComm = 1 					} 
		
		else if( vPrth == 1 )	{ vStr2 += vChar; if( vStr2.split("(").length === vStr2.split(")").length ) 	{ vPrth = 0; vCom3 = 0; vStr2 = '' 		} else { Func_14() } }																										// Пишем текст из () в vStr6, если есть vJoin, и выкидываем его из vStr3
		else if( vChar == "(" && vComm !== 1 )  																{ vPrth = 1; vCom3 = 1; vStr2 += vChar; vStr6 = '';	Func_11(); 		vStr3 += (vStr3.slice(-1) == ' ' ? '' : ' ') + '()'   } 										// Определяем ()

		else if( vNch2.indexOf(vChar) > -1				 && vCom3 !== 1 )  										{ 											 						vStr3 += (vStr3.slice(-1) == ' ' ? '' : ' ') + vChar  }
		else if( vNch1.indexOf(vChar.toLowerCase()) > -1 && vCom3 !== 1 )  										{ if(vWord == -1){ Func_11() }; vStr4 = (vWord == -1 ? '' : vStr4) + vChar; vStr3 += (vStr3.slice(-1) !== ' ' && vWord == -1 ? ' ' : '') + vChar; vWord = i }
		
	} 
	ActiveDocument.CreateVariable("vCommonScriptV1")
	ActiveDocument.Variables("vCommonScriptV1").SetContent(vSub2, true)
//	ActiveDocument.Variables("vStr3").SetContent(vStr3, true)
//	ActiveDocument.Variables("vStr4").SetContent(vStr4, true)
//	ActiveDocument.Variables("vStr5").SetContent(vStr5, true)
//	ActiveDocument.Variables("vSub2").SetContent(vSub, true)
}

function Func_11(){
	var vStr1 = vStr4.toLowerCase()
	var vStr2 = vStr7.split(' ')[vStr7.split(' ').length -2]
	if ( vStr2 !== '=' ) {
			 if( vStr1 == 'left'  			) { vType = 'left' 			; Func_12(1) } 
		else if( vStr1 == 'right'  			) { vType = 'right' 		; Func_12(1) }
		else if( vStr1 == 'outer'  			) { vType = 'outer' 		; Func_12(1) }
		else if( vStr1 == 'join'  			) { vJoin = 'join' 			; Func_12(2) } 
		else if( vStr1 == 'concatenate' 	) { vJoin = 'concatenate' 	; Func_12(2) } 
		else if( vStr1 == 'load' 			) { vLoad = 1				; Func_12(3)
			if ( vPoint > 0 && vJoin == 'join' && vEnd ) { 
				vTab2 = vStr6 == '' ? 'LastTable' : vStr6 
				vShNo = vSub.F(0, vChrNo).split("///&tab").length
				vRwNo = vSub.substring( vSub.lastIndexOf("///&tab"), vChrNo).split( String.fromCharCode(13) ).length
		
				vStr8 = ' CALL S54(\'' + vShNo + ':' + vRwNo + '\', \'' + (vType + vJoin).charAt(0) + '\', \'' + vTab2 + '\')' 
				vPnt1 = vSub2.length - (vSub.length - vPoint); 
				vSub2 = vSub2.slice( 0, vPnt1 ) + vStr8 + ';' + vSub2.slice( vPnt1) 
				vEnd = 0
			}  
			else if( vJoin == '' 			) { 
//				vTab2 = vJoin !== '' ? ( vStr6 == '' ? 'LastTable' : vStr6 ) : ''
			}
		}
		else if( vStr1 == 'from'  			) { vFrom = 'from' 			; Func_13(1) }
		else if( vStr1 == 'resident'  		) { vFrom = 'resident' 		; Func_13(2) } 
		else if( vStr1 == 'autogenerate' 	) { vFrom = 'autogenerate' 	; Func_13(2) } 
	}
	vStr4 = ''; 			
}
function Func_12(vNo){ 
	if ( vPoint == 0 && vEnd ) {
		vColn = vStr7.split(' ')[vStr7.split(' ').length -2] == ':' ? vNo : 0 
		if ( !vColn ) { vPoint = vSub.substring(0, vChrNo).toLowerCase().lastIndexOf( vNo == 1 ? vType : ( vNo == 2 ? vJoin : 'load' ) ) - 1 } 
		else if ( vStr7.split(' ')[vStr7.split(' ').length -3] == '[]' ) { vPoint = vSub.substring(0, vChrNo).toLowerCase().lastIndexOf( '\[' ) - 1; 		vTab1 = vStr5 }
		else { vPoint = vSub.substring(0, vSub.substring(0, vChrNo).lastIndexOf( ':' )).lastIndexOf( vStr7.split(' ')[vStr7.split(' ').length -3] ) - 1; 	vTab1 = vStr7.split(' ')[vStr7.split(' ').length -3] }		
	}
}
function Func_13(){ // Ищем завершение LOAD для добавления теста
	if( !vEnd ) { 
		vPnt1 = vSub2.length - (vSub.length - vChrNo);
		vPnt2 = vSub2.indexOf( ';', vPnt1 ); 
		vSub2 = vSub2.slice( 0,  vPnt2) + ';' + vStr8 + vSub2.slice( vPnt2 ) 
		vEnd = 1
 	} 
}
function Func_14(){
	vStr6 = vStr6 + ( vJoin !== '' && vLoad == 0  ? vChar : '' )
}





// Данная часть для добавления в скрипт двух листов с атоматически сгенерированными модулями. 1 -  загрузка из SQL, 2 - загрузка из qvd

function SQL() { GenerateScript('sql') }
function QVD() { GenerateScript('qvd') }


function GenerateScript(vScriptType) { 
	vScriptType = (vScriptType == null) ? 'qvd' : vScriptType;
	vSheet = (vScriptType == 'sql') ? 'SQL select' : 'QVD load';
	// Сначала уточняем, действительно ли хотим вставить или обновить автоскритпты
	var wshShell 	= new ActiveXObject("WScript.Shell")
    Application.MsgBox("Генератор скрипта", 0 ) 
  	vPopup = wshShell.popup("В модуле скрипта будет создан или заменен лист \""+ vSheet +"\".\r\nПроцесс может занять несколько минут. Хотите продолжить?", 3, '', 1);  
	if(vPopup == 1){ // Если, да, то создаем переменную-маркер для указания в загрузке, что нужен только сгенерированный скрипт
		SetParam('Action', vScriptType == 'sql' ? 'Get queries' : 'Get routines' )
		ActiveDocument.Reload();
		SetAction();
		UpdateScript()		
	}
}

function GenerateScript_(vScriptType) { 
    Application.MsgBox("Генератор скрипта", 0 ) 
	vScriptType = (vScriptType == null) ? 'qvd' : vScriptType;
	vSheet = (vScriptType == 'sql') ? 'SQL select' : 'QVD load';
	// Сначала уточняем, действительно ли хотим вставить или обновить автоскритпты
	var wshShell 	= new ActiveXObject("WScript.Shell")
   	vPopup = wshShell.popup("В модуле скрипта будет создан или заменен лист \""+ vSheet +"\".\r\nПроцесс может занять несколько минут. Хотите продолжить?", 3, '', 1);  
	if(vPopup == 1){ // Если, да, то создаем переменную-маркер для указания в загрузке, что нужен только сгенерированный скрипт
		SetParam('Action', vScriptType == 'sql' ? 'Get queries' : 'Get routines' )
		ActiveDocument.Reload();
		SetAction();
		UpdateScript()		
	}
}

// Ищем 2 переменные 'vLoadScript' и 'vSelectScript' со сгенерированными скриптами, созданными во при загрузке. Текст из каждой из них вставляем в модуль на отдельный лист
function UpdateScript() { //qvlib.msgbox();
    for (var i in array = ['vSelectScript', 'vLoadScript']) {              
        var vSL 		= array[i];  
        var vTb 		= (vSL == 'vLoadScript') ? 'QVD load\r\n' : 'SQL select\r\n'; vTb =  '///$tab ' + vTb;
		var vDatabase 	= GetParam('Database'); //ActiveDocument.Variables("vBaseName").GetContent().string;
        
        if (ActiveDocument.Evaluate("=Len(Trim(" + vSL + "))") > 0) { 
            ActiveDocument.CreateVariable("vScript");
            ActiveDocument.Variables("vScript").SetContent(ActiveDocument.GetProperties().Script, true);
			
			if(vSL == 'vSelectScript') { 
				var vText = "// Раздел сгенерирован " + new Date().toLocaleDateString() + " Содержит sql запросы к 1С "+ vDatabase +", применямые в загрузке. Дополнительная обработка в разделе отсутствует.\r\n";
				vText 	 += "// Данные сохраняются в ..\\QVD\\"+ vDatabase +"\\*.qvd. Названия таблиц и полей соответсвуют 1С. Для обновления используйте \"Generate SQL select\" на форме.\r\n\r\n";
			}else{
				var vText = "// Раздел сгенерирован " + new Date().toLocaleDateString() + " Содержит отформатированные load запросы к ..\\QVD\\"+ vDatabase +"\\*.qvd с комментариями.\r\n\r\n";
				vText 	 += "// Для обновления используйте \"Generate QVD load\" на форме.\r\n\r\n";
			}	
				
				
            if (
                ActiveDocument.Evaluate("=SubStringCount(vScript, '"+ vTb +"')") > 0 //&&
//              ActiveDocument.Evaluate("=SubStringCount(SubField(SubField(vScript, '///$tab "+ vTb +"' & Chr(13), 2), '///', 1), '"+ vText +"')") > 0
            ) {
                var vNewScript = ActiveDocument.Evaluate("=Replace(vScript, '"+ vTb +"' & SubField(SubField(vScript, '"+ vTb +"', 2), '///$tab', 1), '"+ vTb + vText +"' & " + vSL + ")");
            } else {
                var vNewScript = ActiveDocument.GetProperties().Script + vTb + vText + ActiveDocument.Variables(vSL).GetContent().string;
            }
            var Properties = ActiveDocument.GetProperties();
            Properties.Script = vNewScript;
            ActiveDocument.SetProperties(Properties);
       	 	
       	 	ActiveDocument.RemoveVariable("vScript");
        	ActiveDocument.RemoveVariable(vSL);
        }
    }
}







// ========================================== Для обычных приложений

function ExportScript_objShell(){
	Application.MsgBox("Экспорт скрипта", 0 )
	vPopup 			= wshShell.popup("Сохранение скрипта в файл.", 3, '', 1); 
	if(vPopup == 1){
		var objFile = objShell.BrowseForFolder(0, "Выберите файл", "&H4000", "//172.16.245.8/qlikview/txt") 
		if(objFile==null ){  return } // qvlib.MsgBox("Файл не выбран");
		if(!fso.FileExists(objFile.self.path)){ qvlib.MsgBox("Файл не найден"); return }	
		vPath = objFile.self.path
		writeBinary(vPath, 2, ActiveDocument.GetProperties().Script)
		Application.MsgBox("Готово!");
	}
}

function ImportScript_objShell(){ // Падает в ошибку с обычными файлами
    Application.MsgBox("Импорт скрипта", 0 ) 
  	vPopup 				= wshShell.popup("Загрузка и замена всего скрипта из файла.", 3, '', 1); 
	if(vPopup == 1){
		var objFile = objShell.BrowseForFolder(0, "Выберите файл", "&H4000", "//172.16.245.8/qlikview/qvs/") 
		if(objFile==null ){  return } // qvlib.MsgBox("Файл не выбран");
		if(!fso.FileExists(objFile.self.path)){ qvlib.MsgBox("Файл не найден"); return }	
		vPath = objFile.self.path
		var Properties 		= ActiveDocument.GetProperties();
		Properties.Script 	= readBinary( vPath, 2 );   
		ActiveDocument.SetProperties(Properties);
		Application.MsgBox("Готово!");		
	}
}

function ImportScript(){
    Application.MsgBox("Импорт скрипта", 0 ) 
	var result = wshShell.popup("Загрузить по умолчанию?", 1, '', 3);
	if ( result == 6 ) {
		var path = qvsPath
	} else if ( result == 7 ) {
		var path = wshShell.Exec("mshta.exe \"about:<input type=file id=f><script>resizeTo(0,0);f.click();new ActiveXObject('Scripting.FileSystemObject').GetStandardStream(1).WriteLine(f.value);close();</script>\"").StdOut.ReadLine()
	}
	if(  typeof path !== 'undefined' && path.length > 0 ) {
		var Properties 		= ActiveDocument.GetProperties();
		Properties.Script 	= readBinary( path, 2 );  
		ActiveDocument.SetProperties(Properties);
		SetParam('LoadMode', 'Internal')
	}	
}

function ExportScript(){
    if ( (document.Evaluate("=Len(vMetas)") > 0 && document.GetProperties().Script.indexOf('Execute') == -1) || (document.Evaluate("=Len(vMetas)") == 0 && document.GetProperties().Script.length < 20 ) ) { 
		qvlib.msgbox ("Сохранять нечего")
	} else {
		Application.MsgBox("Экспорт скрипта", 0 ) 
		var result = wshShell.popup("Сохранить по умолчанию?", 1, '', 3);
		if ( result == 6 ) {
			var path = qvsPath
		} else if ( result == 7 ) {
			var path = wshShell.Exec("mshta.exe \"about:<input type=file id=f><script>resizeTo(0,0);f.click();new ActiveXObject('Scripting.FileSystemObject').GetStandardStream(1).WriteLine(f.value);close();</script>\"").StdOut.ReadLine()
		}
		if( typeof path !== 'undefined' && path.length > 0 ) {
			writeBinary(path, 2, ActiveDocument.GetProperties().Script)
	//		Application.MsgBox("Готово!");
		}			
	}
}

function DeleteScript(){
    var Properties = ActiveDocument.GetProperties(); 
 	Properties.Script = '///$tab Main';
    ActiveDocument.SetProperties(Properties);
	SetParam('LoadMode', 'External')
}


function SetLoadMode(){
	var loadMode = GetParam('LoadMode') == 'External' ? 'Internal' : 'External'
	if( loadMode == 'Internal' ) { 
		if( (document.Evaluate("=Len(vMetas)") > 0 && document.GetProperties().Script.indexOf('Execute') == -1) || (document.Evaluate("=Len(vMetas)") == 0 && document.GetProperties().Script.length < 20 ) ) {  //qvlib.msgbox(document.Evaluate("=Len(vMetas)"));  //qvlib.msgbox(document.GetProperties().Script.indexOf('Execute'))
			loadMode = 'External';
			qvlib.msgbox('В модуле ничего нет')
		}
	}
	SetParam('LoadMode', loadMode)
}


function setApp(){
	try {
        // Проверяем, существует ли закладка BM555
        var bookmarkId = document.GetBookmarkId("BM555");
		//qvlib.MsgBox(bookmarkId);
        if (bookmarkId) {
            document.RemoveUserBookmark("BM555"); // Удаляем, если существует
        } 
	} catch (e) {
        // Логируем ошибку для отладки (опционально)
        // MsgBox("Произошла ошибка: " + e.description);
    }
	document.CreateUserBookmark("BM555");
	document.ClearAll();
	setChartExpressions();
	setChartDimensions();
	setMultiBoxFields();
	document.RecallUserBookmark("BM555");
	document.RemoveUserBookmark("BM555");
}

function setChartExpressions() { //qvlib.msgbox(expressions.count)
    var chart 																			= document.getSheetObject("CH00"); 
    var chartProperties 																= chart.getProperties();
    var expressions 																	= chartProperties.Expressions;
    var expressionsField																= document.evaluate("=If(Count(E1) > 0, 'E1', If(Count(Exp) > 0, 'Exp', If(Count(Exp_1) > 0, 'Exp_1')))");
    var expressionFields																= document.evaluate("=Concat(Distinct "+ expressionsField +", ',', Aggr(RowNo(), "+ expressionsField +"))").split(',');

    for (var i = 0; i < expressions.count; i++) {  // удаляем имеющиеся выражения 
        chart.removeExpression(0);
        chartProperties 																= chart.getProperties();
        chart.setProperties(chartProperties);
    }

    for (var i = 0; i < expressionFields.length; i++) {	// добавляем пустые выражения, необходимое количество
        chart.addExpression("0");
        chartProperties 																= chart.getProperties();
        chart.setProperties(chartProperties);
    }

    chartProperties 																	= chart.getProperties();
    expressions 																		= chartProperties.Expressions;

    for (var i = 0; i < expressionFields.length; i++) { //qvlib.msgbox( expressionFields[i] )
        var expressionData 																= expressions.Item(i).Item(0).Data.ExpressionData;
        expressionData.Comment 															= " ";
        expressionData.Definition.v 													= 1; //ActiveDocument.evaluate("=Only({1<[Expressions.Number]={" + (i + 1) + "}>} Trim([$Expressions.Definition]))");
        expressionData.Enable 															= true;
        expressions.Item(i).Item(0).Data.EnableCondition.Expression 					= "$(vSelected("+ expressionsField +", " + expressionFields[i] + "))";
        expressions.Item(i).Item(0).Data.EnableCondition.Type 							= 2;
        // expressions.Item(i).Item(0).Data.ExpressionData.Comment 						= "";
        var expressionVisual 															= expressions.Item(i).Item(0).Data.ExpressionVisual;
        expressionVisual.Label.v 														= expressionFields[i];
        expressionVisual.LabelAdjust 													= 1; // 1 = center, 0 = left, 2 = right
        // expressionVisual.NumAdjust 													= 1;
        // expressionVisual.TextAdjust 													= 1;
        expressionVisual.NumberPresentation.Fmt 										= "# ##0"; 
        expressionVisual.NumberPresentation.Type 										= 10;

        var attributeExpressions 														= expressions.Item(i).Item(0).Data.AttributeExpressions;
        // attributeExpressions.BkgColorExp.Definition.v 								= "$(=$(expressions.BkgColor(" + (i + 1) + ")))";
        // attributeExpressions.TextColorExp.Definition.v 								= "$(=$(expressions.TextColor(" + (i + 1) + ")))";
        // attributeExpressions.TextFmtExp.Definition.v 								= "$(=$(expressions.TextFmt(" + (i + 1) + ")))";
	
    }

    chart.setProperties(chartProperties);
	
/* 	for (var i = 0; i < expressionFields.length; i++) {
		var field																		= document.GetField(expressionFields[i]);qvlib.msgbox( expressionFields[i] )
		var fieldProperties																= field.GetProperties(); 
		// fieldProperties.NumberPresentation.Dec 											= asc(".");
		fieldProperties.NumberPresentation.Fmt 											= "# ##0"; 
		// fieldProperties.NumberPresentation.nDec 										= 2;
		// fieldProperties.NumberPresentation.Thou 										= asc(" "); 
		fieldProperties.NumberPresentation.Type 										= 10 ;      // 11 fixed decimal
		fieldProperties.NumberPresentation.UseThou 										= 1;
		field.SetProperties(fieldProperties)
	} */
}

function setChartDimensions() {
    var chart 																			= document.getSheetObject("CH00"); 
    var chartProperties 																= chart.getProperties();
    var dimensions 																		= chartProperties.Dimensions;
    var dimensionsField																	= document.evaluate("=If(Count(D1) > 0, 'D1', If(Count(Dim) > 0, 'Dim', If(Count(Dim_1) > 0, 'Dim_1')))");
	var dimensionFields																	= document.evaluate("=Concat({1} Distinct "+ dimensionsField +", ',', Aggr(RowNo(), "+ dimensionsField +"))").split(',');
	var documentFields																	= document.evaluate("=Concat({1} $Field, ',')").split(',');
	var dateFields																		= document.evaluate("=Concat({1< $Table = {'Календарь'} >} $Field, ',')").split(',');
	var j 																				= 0;
	
    for (var i = 0; i < dimensions.Count; i++) {
        chart.removeDimension(0);
        chartProperties 																= chart.getProperties();
        chart.setProperties(chartProperties);
    }
	
    for (var i = 0; i < dimensionFields.length; i++) {   // qvlib.msgbox( dimensionFields[i] )
    	if( documentFields.contains( dimensionFields[i] )) {
	        chart.addDimension(dimensionFields[i]); 
	        chartProperties 															= chart.getProperties(); 
	        chart.setProperties(chartProperties); 	
	        chartProperties 															= chart.getProperties(); //qvlib.msgbox( j )
	        chartProperties.Dimensions(j).EnableCondition.Expression 					= "$(vSelected("+ dimensionsField +", " + dimensionFields[i] + "))"; 
	        chartProperties.Dimensions(j).EnableCondition.Type 							= 2;
	        chartProperties.Dimensions(j).Title.v 										= dimensionFields[i];
	
	        chartProperties.Dimensions(j).ShowPartialSums 								= true;
        
	        if (dateFields.contains(dimensionFields[i])) {
	            chartProperties.Dimensions(j).LabelAdjust 								= 1;
	            chartProperties.Dimensions(j).TextAdjust 								= 1; // 1 = center, 0 = left, 2 = right
	            chartProperties.Dimensions(j).NumAdjust 								= 1;
	            chartProperties.Dimensions(j).SortCriteria.SortByLoadOrder 				= 1;
	            chartProperties.Dimensions(j).SortCriteria.SortByAscii 					= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByFrequency 				= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByNumeric 				= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByState 					= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByExpression 			= 0;
	            chartProperties.Dimensions(j).SortCriteria.Expression.v 				= "";
	        } else {
	            chartProperties.Dimensions(j).LabelAdjust 								= 1;
	            chartProperties.Dimensions(j).TextAdjust 								= 0;
	            chartProperties.Dimensions(j).NumAdjust 								= 0;
	            chartProperties.Dimensions(j).DropdownSelect 							= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByLoadOrder 				= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByAscii 					= 1;
	            chartProperties.Dimensions(j).SortCriteria.SortByFrequency 				= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByNumeric 				= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByState 					= 0;
	            chartProperties.Dimensions(j).SortCriteria.SortByExpression 			= 0;
	            chartProperties.Dimensions(j).SortCriteria.Expression.v 				= "";
	        } 
			j += 1;
        }
        
        chart.setProperties(chartProperties);
    }
    
    chart.setProperties(chartProperties);
}
 
function setMultiBoxFields() {
    var multiBox 																		= document.getSheetObject("MB00"); 
    var multiBoxProperties 																= multiBox.getProperties();
    var multiBoxRowCount																= multiBox.getRowCount();
    var dimensionsField																	= document.evaluate("=If(Count(D1) > 0, 'D1', If(Count(Dim) > 0, 'Dim', If(Count(Dim_1) > 0, 'Dim_1')))");
	var dimensionFields																	= document.evaluate("=Concat({1} Distinct "+ dimensionsField +", ',', Aggr(RowNo(), "+ dimensionsField +"))").split(',');
	var documentFields																	= document.evaluate("=Concat({1} $Field, ',')").split(',');
	var j 																				= 0;

    for (var i = 0; i < multiBoxRowCount; i++) {
        multiBox.removeField(0);
        multiBoxProperties 																= multiBox.getProperties();
        multiBox.setProperties(multiBoxProperties);
    }

    for (var i = 0; i < dimensionFields.length; i++) { 
    	if( documentFields.contains( dimensionFields[i] )) { //qvlib.msgbox( dimensionFields[i] )
	        multiBox.addField( dimensionFields[i] );
	        multiBoxProperties 															= multiBox.getProperties();
	        multiBox.setProperties(multiBoxProperties);
	        multiBoxProperties 															= multiBox.getProperties();
	        multiBoxProperties.MemberAttributes.Item(j).Label.v 						= dimensionFields[i]; 
	        multiBoxProperties.MemberAttributes.Item(j).SortCriteria.SortByState 		= true;
	        multiBox.setProperties(multiBoxProperties);
			j += 1;
		}
    }
    multiBox.setProperties(multiBoxProperties);
}


function EditMode() {
	
	if ( document.Evaluate("=If(WildMatch(OSUser(), '*andrey.krylov*', '*qlikbot*'), 1, 0)") == 1  ) {
		if (GetParam('EditMode') == 0) {
			SetParam('EditMode', 1)
			dif = -64
		} else {
			SetParam('EditMode', 0)
			dif = 64
		}

		var object = ActiveDocument.getSheetObject("CH01") 
		rect = object.GetRect()
		rect.Width =  rect.Width + dif
		object.SetRect(rect)	
		
		var object = ActiveDocument.getSheetObject("LB04")
		rect = object.GetRect()
		rect.Left = rect.Left + dif
		object.SetRect(rect)

	//		rect.Height =  200
	//		rect.Top =  200
	}
}


function getMeta() {
	var exec = new ActiveXObject("WScript.Shell");
	
	// Пример переменных (могут содержать пробелы)
	var param1 = GetParam('QvdPath') + '\\Meta';
	var param2 = GetParam('Server');
	var param3 = GetParam('Database');
	var param4 = GetParam('Login');
	var param5 = GetParam('Password');
	var param6 = GetParam('SQL');
	
	// Экранируем КАЖДЫЙ аргумент кавычками (даже если пробелов нет)
	var args = [
	    '"' + param1.replace(/"/g, '\\"') + '"',  
	    '"' + param2.replace(/"/g, '\\"') + '"',
	    '"' + param3.replace(/"/g, '\\"') + '"',
	    '"' + param4.replace(/"/g, '\\"') + '"',
	    '"' + param5.replace(/"/g, '\\"') + '"',
		'"' + param6.replace(/"/g, '\\"') + '"'
	].join(" ");
	
	// Пути к exe и скрипту (уже с кавычками, если есть пробелы)
	var exePath 	= '"' + GetParam('GitPath') + '/PY/runpy.exe"'; 
	var pyScript 	= '"' + GetParam('GitPath') + '/PY/Meta_exe.py"';
	
	// Собираем команду (без лишних кавычек!)
	var command 	= exePath + " " + pyScript + " " + args;
	
	// Запускаем (добавляем cmd /c и общие кавычки)
	var result 		= exec.Run('cmd /c "' + command + '" 2>&1', 0, true);
}
