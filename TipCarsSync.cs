using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Web;
using System.Xml.Serialization;
using Funglovky.Services;

namespace Funglovky.TipCarsSync
{   
    /// <summary>
    /// Provádí pomocí metody Execute() import dat ze serveru TipCars.
    /// </summary>
    public class Import
    {      
        /// <summary>
        /// Fronta pro stahování fotografií. Slouží k zařazování požadavků na stažení s zpracování
        /// fotografie s možností určení počtu pokusů o stažení. 
        /// </summary>
        public class PhotosQueue
        {
            /// <summary>
            /// Data fronty. Jsou vyčleněna do samostatně serializovatelného typu.
            /// </summary>
            public class Requests
            {
                /// <summary>
                /// Požadavek na stažení jedné fotografie.
                /// </summary>            
                public class Request
                {                  
                    /// <summary>
                    /// Počet pokusů o stažení
                    /// </summary>
                    public int TryCount { get; set; }
                 
                    /// <summary>
                    /// Název fotky.
                    /// </summary>
                    public string FileName { get; set; }

                    // Je fotka implicitní?
                    public bool IsDefault { get; set; }

                    /// <summary>
                    /// ID lokální fotografie nebo null pro vložení
                    /// </summary>
                    public int? photoId { get; set; }

                    /// <summary>
                    /// ID auta.
                    /// </summary>
                    public int CarID { get; set; }

                    /// <summary>
                    /// ID auta.
                    /// </summary>
                    public string CarTitle { get; set; }

                    /// <summary>
                    /// DatumČas poslední aktualizace na TipCars
                    /// </summary>
                    public DateTime LastUpdated { get; set; }
                }

                [XmlElement("item")]
                public List<Request> Items { get; set; }

                public Requests()
                {
                    this.Items = new List<Request>();
                }
            }

            private readonly string requestsXmlFile;
            private Requests requestsQueue;

            /// <summary>
            /// Načte frontu z úložiště.
            /// </summary>
            private void Load()
            {
                if (File.Exists(requestsXmlFile))
                {
                    try
                    {
                        XMLLoader<Requests> ldr = new XMLLoader<Requests>();
                        this.requestsQueue = ldr.Load(requestsXmlFile);                      
                    }
                    catch
                    {
                        // když se nezdaří načíst pravděpodobně pro špatný formát, tak frontu smazat
                        try { File.Delete(requestsXmlFile); } catch {}
                        requestsQueue = new Requests();
                    }
                }
                else
                    requestsQueue = new Requests();
            }

            /// <summary>
            /// Uloží frontu do úložiště.
            /// </summary>
            private void Save()
            {                
                using (StreamWriter writer = new StreamWriter(requestsXmlFile, false))
                {
                    XmlSerializer serializer = new XmlSerializer(typeof(Requests));
                    serializer.Serialize(writer, requestsQueue);
                }                                
            }

            /// <summary>
            /// Získá seznamu požadavků na stažení, tedy ty, které nemají vyčerpán počet pokusů.
            /// Zároveň jsou odstraněny požadavky, jejichž counter dosáhl nuly.
            /// </summary>           
            public List<Requests.Request> PopRequests()
            {
                var requests = requestsQueue.Items.Where((r) => { return r.TryCount > 0; });
                var result = requests.ToList();
              
                foreach (var request in requests)
                    request.TryCount--;

                requestsQueue.Items.Where((r) => { return r.TryCount == 0; }).ToList()
                    .ForEach((r) => { requestsQueue.Items.Remove(r); });                                

                Save();
                return result;
            }

            /// <summary>
            /// Zařazení nového požadavku na stažení.
            /// </summary>            
            public void PushRequest(Requests.Request request)
            {
                if (request.TryCount == 0)
                    request.TryCount = 3;

                var existingReq = this.requestsQueue.Items.FirstOrDefault((r) => r.FileName == request.FileName);
                if (existingReq != null)
                    this.requestsQueue.Items.Remove(existingReq);

                this.requestsQueue.Items.Add(request);
                this.Save();
            }

            /// <summary>
            /// Odstranění fotografie z fronty pro stažení.
            /// </summary>            
            public void RemovePhoto(string photoName)
            {
                var existingReq = this.requestsQueue.Items.FirstOrDefault((r) => r.FileName == photoName);
                if (existingReq != null)
                {
                    this.requestsQueue.Items.Remove(existingReq);
                    Save();
                }               
            }

            /// <summary>
            /// Konstruktor.
            /// </summary>            
            public PhotosQueue()
            {
                requestsXmlFile = Path.Combine(HttpContext.Current.ApplicationInstance.Server.MapPath("~/App_Data"), "tipcars-photos-requests.xml");
                Load();
            }
            
            public static void Test()
            {
                string requestsXmlFile = Path.Combine(HttpContext.Current.ApplicationInstance.Server.MapPath("~/App_Data"), "tipcars-photos-requests.xml");
                File.Delete(requestsXmlFile);

                if (File.Exists(requestsXmlFile))
                    throw new ApplicationException(String.Format("Nezdařilo se smazat soubor \"{0}\" před testem.", requestsXmlFile));

                try
                {

                    PhotosQueue.Requests.Request request = new Requests.Request();
                    request.FileName = "photoname";
                    request.TryCount = 3;

                    var queue = new PhotosQueue();
                    queue.PushRequest(request);

                    // test přítomnosti ve frontě 1
                    queue = new PhotosQueue();
                    if (!queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek není nalezen ve frontě při prvním Pop požadavku.");

                    // test přítomnosti ve frontě 2             
                    queue = new PhotosQueue();
                    if (!queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek není nalezen ve frontě po druhém Pop požadavku.");

                    // test přítomnosti ve frontě 3   
                    queue = new PhotosQueue();
                    if (!queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek není nalezen ve frontě po třetím Pop požadavku.");

                    // test nepřítomnosti ve frontě po třech pokusech na pop
                    queue = new PhotosQueue();
                    if (queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek je ve frontě i po trojnásobném vyzvednutí.");

                    // test RemovePhoto()
                    queue = new PhotosQueue();
                    queue.PushRequest(request);
                    queue = new PhotosQueue();
                    queue.RemovePhoto(request.FileName);
                    if (queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Požadavek zůstal ve frontě po volání RemovePhoto()");


                    // ***** Totéž, ale bez testování perzistence ****** //

                    queue = new PhotosQueue();
                    queue.PushRequest(request);

                    // test přítomnosti ve frontě 1                
                    if (!queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek není nalezen ve frontě při prvním Pop požadavku (test bez perzistence).");

                    // test přítomnosti ve frontě 2                             
                    if (!queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek není nalezen ve frontě po druhém Pop požadavku (test bez perzistence).");

                    // test přítomnosti ve frontě 3                   
                    if (!queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek není nalezen ve frontě po třetím Pop požadavku (test bez perzistence).");

                    // test nepřítomnosti ve frontě po třech pokusech na pop                
                    if (queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Zařazený požadavek je ve frontě i po trojnásobném vyzvednutí (test bez perzistence).");

                    // test RemovePhoto()                  
                    queue.PushRequest(request);                    
                    queue.RemovePhoto(request.FileName);
                    if (queue.PopRequests().Exists((r) => r.FileName == "photoname"))
                        throw new ApplicationException("Požadavek zůstal ve frontě po volání RemovePhoto() (test bez perzistence)");

                }
                finally
                {
                    // úklid
                    File.Delete(requestsXmlFile);                  
                }               
            }
        }        

        /// <summary>
        /// Pomocné číselníky pro přiřazování hodnot auta při importu.
        /// </summary>
        private class MatchLists
        {
            public IEnumerable<ListSupplier> Suppliers { get; set; }
            public IEnumerable<ListModel> Models { get; set; }
            public IEnumerable<VModel> VModels { get; set; }
            public IEnumerable<VModelBodyStyleRecord> Bodies { get; set; }
            public IEnumerable<ListColor> Colors { get; set; }
            public IEnumerable<CarFuelRecord> Fuels { get; set; }
            public IEnumerable<ListParam> Params { get; set; }

            public MatchLists()
            {
                // Vylučujeme všechny záznamy, u kterých není uveden žádný kód TipCars
                this.Suppliers = new ListSuppliersDB().List().Where(s => !String.IsNullOrWhiteSpace(s.TcCodes)).ToArray();
                this.Models = new ListModelsDB().List().Where(m => !String.IsNullOrWhiteSpace(m.TcCodes)).ToArray();
                this.VModels = new VModelsDB().List(null, null);
                this.Bodies = new VModelsDB().ListBodies().Where(b => !String.IsNullOrWhiteSpace(b.TcCodes)).ToArray();
                this.Colors = new ListColorsDB().List().Where(c => !String.IsNullOrWhiteSpace(c.TcCodes)).ToArray();
                this.Fuels = new CarsDB().ListFuel().Where(f => !String.IsNullOrWhiteSpace(f.TcCodes)).ToArray();
                this.Params = new ListParamsDB().List().Where(p => !String.IsNullOrWhiteSpace(p.TcCodes)).ToArray();
            }
        }
             
        private Statistics statistics;

        /// <summary>
        /// Konstruktor.
        /// </summary>        
        public Import()
        {                                       
            this.statistics = new Statistics();
        }               

        /// <summary>
        /// Spuštění importu.        
        /// </summary>        
        public bool Execute(out string message)
        {            
            MatchLists matchLists = new MatchLists();     

            string lockFile = Path.Combine(HttpContext.Current.ApplicationInstance.Server.MapPath("~/App_Data"), "tipcars-import.lock");

            if (File.Exists(lockFile))
            {
                message = String.Format("Import již probíhá. Byl nalezen zámek \"{0}\"", lockFile);
                return false;
            }

            File.WriteAllText(lockFile, "");

            try
            {
                Log.SectionBegin("Zahajuji import ze serveru TipCars v čase " + DateTime.Now.ToString());
                this.statistics = new Statistics();

                try
                {
                    // Načtení seznamu dealerů, kteří mají zapnutou TipCars synchronizaci            
                    var activeDealers = this.GetDealersImportingTipCars();

                    if (activeDealers.Count() > 0)
                    {
                        // načtení aktualizací inzercí firem ze serveru TipCars            
                        var xmlLastUpdates = this.LoadXmlUpdates(activeDealers
                            .Select(d => d.TcClientID)
                            .Distinct());

                        // sestavit seznam dealerů s aktualizacemi
                        IEnumerable<string> dealersToUpdate = this.GetTcIDsWithUpdates(activeDealers, xmlLastUpdates, matchLists);

                        var photosQueue = new PhotosQueue();

                        if (dealersToUpdate.Count() > 0)
                        {
                            // Načtení veškeré požadované inzerce ze serveru TipCars                
                            var xmlAdverts = LoadXmlAdverts(dealersToUpdate);

                            // Provést jednosměrnou synchronizaci aut z načtené inzerce do databáze                        
                            this.SyncCars(xmlAdverts, activeDealers, xmlLastUpdates, photosQueue, matchLists);
                        }

                        // Provést aktualizaci fotografií z fronty
                        this.ImportPhotos(photosQueue);

                        string dtNowStr = DateTime.Now.ToString();
                        this.statistics.ImportTime = dtNowStr;
                        this.statistics.Save();

                        if (dealersToUpdate.Count() == 0 && this.statistics.PhotosInsertedNum == 0 && this.statistics.PhotosUpdatedNum == 0)
                            message = "Inzerce je aktuální, žádná data pro import.";
                        else
                            message = "Import byl úspěšně dokončen v čase " + dtNowStr + "";

                    }
                    else
                        message = "Žádný dealer nemá aktivován import dat.";

                    Log.SectionEnd(message);
                    return true;
                }
                catch (Exception ex)
                {
                    message = "Import skončil s chybou: " + ex.Message + " Čas: " + DateTime.Now.ToString();
                    Log.SectionEnd(message);
                    Log.NotifyException(ex);
                    return false;
                }
            }
            finally
            {
                File.Delete(lockFile);
            }
        }      

        /// <summary>
        /// Načte vzdálená data o posledních aktualizacích.
        /// </summary>
        private TipCarsSync.XmlUpdates LoadXmlUpdates(IEnumerable<string> dealersTcIDs)
        {
            Log.LineBegin("Načítám seznam aktualizací ze serveru TipCars...");
            try
            {
                XmlUpdates result = XmlUpdates.Load(dealersTcIDs);
                Log.LineOK();
                return result;
            }
            catch(Exception ex)
            {
                Log.LineErr(ex.Message);                
                Log.NotifyException(ex);
                this.statistics.ErrorsNum++;
                throw;
            }
        }

        /// <summary>
        /// Načte všechnu inzerci pro zadané dealery.
        /// </summary>        
        private TipCarsSync.XmlAdverts LoadXmlAdverts(IEnumerable<string> dealersTcIDs)
        {
            Log.LineBegin("Načítám inzertní data dealerů ze serveru TipCars...");
            try
            {
                XmlAdverts result = XmlAdverts.Load(dealersTcIDs);
                Log.LineOK();
                return result;
            }
            catch (Exception ex)
            {
                Log.NotifyException(ex);
                Log.LineErr(ex.Message);
                this.statistics.ErrorsNum++;
                throw;
            }
        }       

        /// <summary>
        /// Vrátí seznam ID všech dealerů, jejichž data se importují.
        /// </summary>        
        private IEnumerable<global::Dealer> GetDealersImportingTipCars()
        {     
            // omezujeme se jen na Dealery importující data z TipCars.
            Log.LineBegin("Načítám seznam dealerů s aktivovaným importem TipCars...");
            try
            {
                var result = new DealersDB()
                    .List()
                    .Where(d => d.TcEnabled && !String.IsNullOrWhiteSpace(d.TcClientID));                   
                Log.LineOK("deale".Pluralize(result.Count(), "r", "ři", "rů"));
                return result;
            }
            catch (Exception ex)
            {
                Log.NotifyException(ex);
                Log.LineErr(ex.Message);
                throw;
            }            
        }

        /// <summary>
        /// Pro existující dealery activeDealers a načtený seznam změněných inzercí vrací seznam dealerů (TipCars ID's),
        /// jejichž data má symsl vyžádat
        /// </summary>        
        private IEnumerable<string> GetTcIDsWithUpdates(IEnumerable<global::Dealer> dealers, XmlUpdates lastUpdates, MatchLists matchList)
        {            
            List<string> dealersToLoad = new List<string>();
            foreach (var company in lastUpdates.Items)
            {
                try
                {
                    if (company.ErrorMessage != null)
                    {
                        Log.Error(String.Format("Nezdařilo se získat data o aktualizacích firmy \"{0}\". Server TipCars vrátil chybu: {1}", company.ClientID, company.ErrorMessage));
                        this.statistics.ErrorsNum++;
                        continue;
                    }
                                   
                    // více dealerů může sdílet stejný TcClientID. Typicky existuje jeden dealer pro každou značku auta,
                    // ač se jedná fyzicky o jednoho dealera.
                    foreach (var dealer in dealers.Where(d => d.TcClientID == company.ClientID))
                    {
                        Log.LineBegin(String.Format("Jsou k dispozici aktualizace pro dealera \"{0}\" [{1}]?", dealer.Name, matchList.Suppliers.FirstOrDefault(s => s.SuppID == dealer.SuppID).Title));

                        if (dealer.TcLastUpdated == null || (dealer.TcLastUpdated < company.LastUpdatedDateTime))
                        {
                            if (!dealersToLoad.Exists(d => d == dealer.TcClientID))
                                dealersToLoad.Add(dealer.TcClientID);
                            Log.LineYes();
                        }
                        else
                        {
                            Log.LineNo();
                        }
                    }
                    
                }
                catch (Exception ex)
                {
                    Log.Error(String.Format("Chyba při zjišťování aktualizací pro TipCars klienta č. \"{0}\": {1}", company.ClientID, ex.Message));
                    Log.NotifyException(ex);
                    this.statistics.ErrorsNum++;
                    continue;
                }
            }

            return dealersToLoad;
        }

        /// <summary>
        /// Provede vlastní přenos aut a fotek inzerce xmlAdverts + filtruje pouze pro dealery activeDealers.
        /// </summary>        
        private void SyncCars(XmlAdverts xmlAdverts, IEnumerable<global::Dealer> dealers, XmlUpdates xmlUpdates, PhotosQueue photosQueue, MatchLists matchLists)
        {
            Log.SectionBegin("Zahajuji zpracování stažených aktualizací...");

            try
            {                                                       
                foreach (var xmlDealer in xmlAdverts.Items)
                {
                    // chráněný blok, aby chyba v cyklu neshodila celý cyklus
                    try
                    {
                        if (xmlDealer.Info.ErrorMessage != null)
                        {
                            Log.Error(String.Format("Nezdařilo se získat inzertní data firmy \"{0}\". Server TipCars vrátil chybu: {1}", xmlDealer.Info.ClientID, xmlDealer.Info.ErrorMessage));
                            this.statistics.ErrorsNum++;
                            continue;
                        }
                      
                        foreach (var dealer in dealers.Where(d => d.TcClientID == xmlDealer.Info.ClientID))
                        {

                            Log.SectionBegin(String.Format("Importuji vozy dealera \"{0}\" [{1}]", dealer.Name, matchLists.Suppliers.FirstOrDefault(s => s.SuppID == dealer.SuppID).Title));

                            // v první etapě vytváříme nebo aktualizujeme auta
                            foreach (var xmlCar in xmlDealer.Cars.Items)
                            {
                                // chráněný blok, aby výjimka nepřerušila celý cyklus
                                try
                                {
                                    // Vyloučíme auta, která nejsou evidována jako nová
                                    if (xmlCar.CategoryKey != "N")
                                    {
                                        Log.Info(String.Format("Přeskakuji nenový vůz \"{0}\"", xmlCar.ToString()));
                                        continue;
                                    }

                                    // Vyloučíme auta jiných značek, než jaké má dealer nabízet
                                    if (String.IsNullOrEmpty(xmlCar.ManufacturerKey))
                                    {
                                        Log.Warning(String.Format("Nelze zpracovat vůz \"{0}\", element \"manufacturer_klic\" je prázdný.", xmlCar.ToString()));
                                        this.statistics.CarsSkipedNum++;
                                        continue;
                                    }

                                    var supplier = matchLists.Suppliers.FirstOrDefault(
                                        s => s.TcCodes.Split(',').Contains(xmlCar.ManufacturerKey));

                                    if (supplier == null)
                                    {
                                        Log.Warning(String.Format("Nelze zpracovat vůz \"{0}\", protože kód jeho značky \"{1}\" není nastaven v žádném záznamu místního číselníku značek.", xmlCar.ToString(), xmlCar.ManufacturerKey));
                                        this.statistics.WarningsNum++;
                                        this.statistics.CarsSkipedNum++;
                                        continue;
                                    }

                                    if (supplier.SuppID != dealer.SuppID)
                                    {
                                        Log.Info(String.Format("Přeskakuji vůz \"{0}\", protože neodpovídá značce spravované dealerem.", xmlCar.ToString()));
                                        this.statistics.CarsSkipedNum++;
                                        continue;
                                    }

                                    Log.LineBegin(String.Format("Importuji vůz \"{0}\"", xmlCar.ToString()));
                                    try
                                    {

                                        // Update nebo inzert podle existence/neexistence                                                   
                                        CarData localCar = this.LoadCar(xmlCar.CarID, dealer.DealerID.Value);
                                        int carId;
                                        if (localCar != null)
                                        {
                                            this.UpdateCar(localCar, xmlCar, dealer.DealerID.Value, matchLists);
                                            carId = localCar.CarID.Value;
                                        }
                                        else
                                        {
                                            carId = this.InsertCar(xmlCar, dealer.DealerID.Value, matchLists);
                                            localCar = new CarsDB().Get(carId)[0];
                                        }

                                        // Nastavit parametry
                                        this.SaveCarParams(xmlCar, carId, matchLists.Params);

                                        Log.LineOK();

                                        // Zařadit požadavek na synchronizaci fotek                                 
                                        this.QueueOrDeleteCarPhotos(xmlCar, localCar, photosQueue);
                                    }
                                    catch (BadTipCarsData ex)
                                    {
                                        Log.LineWarning(ex.Message);
                                        this.statistics.WarningsNum++;
                                        this.statistics.CarsSkipedNum++;
                                        continue;
                                    }
                                    catch (CannotMatchCar ex)
                                    {
                                        Log.LineWarning(ex.Message);
                                        this.statistics.WarningsNum++;
                                        this.statistics.CarsSkipedNum++;
                                        continue;
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.LineErr(String.Format("Nelze naimportovat vůz \"{0}\", protože při importu nastala neočekávaná chyba: {1}", xmlCar.ToString(), ex.Message));
                                        Log.NotifyException(ex);
                                        this.statistics.ErrorsNum++;
                                        this.statistics.CarsSkipedNum++;
                                        continue;
                                    }

                                }
                                catch (Exception ex)
                                {
                                    Log.Error(String.Format("Nelze zpracovat vůz \"{0}\", protože nastala neočekávaná chyba: {1}", xmlCar.ToString(), ex.Message));
                                    Log.NotifyException(ex);
                                    this.statistics.ErrorsNum++;
                                    this.statistics.CarsSkipedNum++;
                                    continue;
                                }
                            }

                            // v druhé etapě smažeme, auta, která nejsou přítomná v aktualizaci
                            this.DeleteOthersCars(xmlDealer.Cars.Items, dealer.DealerID.Value);

                            // zaznamenáme poslední datum aktualizace u dealera 
                            var receivedDealerUpdateRecord = xmlUpdates.Items.FirstOrDefault(d => d.ClientID == xmlDealer.Info.ClientID);
                            if (receivedDealerUpdateRecord != null)
                            {
                                Log.LineBegin(String.Format("Zaznamenávám poslední datum aktualizace \"{0}\" u dealera \"{1}\"", receivedDealerUpdateRecord.LastUpdatedDateTime.ToString(), xmlDealer.Info.Name));
                                try
                                {
                                    new DealersDB().UpdateTcLastUpdated(dealer.DealerID.Value, receivedDealerUpdateRecord.LastUpdatedDateTime);
                                    Log.LineOK();
                                }
                                catch (Exception ex)
                                {
                                    this.statistics.ErrorsNum++;
                                    Log.LineErr(ex.Message);
                                    Log.NotifyException(ex);
                                }
                            }

                            Log.SectionEnd();
                            this.statistics.UpdatedDealersNum++;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.SectionEnd(String.Format("Nastala neočekávaná chyba při importu vozů dealera \"{0}\". {1}", xmlDealer.Info.Name, ex.Message));
                        Log.NotifyException(ex);
                        this.statistics.ErrorsNum++;
                        continue;
                    }
                }

                Log.SectionEnd("Zpracování aktualizací bylo úspěšně dokončeno.");               
            }
            catch (Exception ex)
            {                               
                Log.SectionEnd("Zpracování aktualizací skončilo chybou: " + ex.Message);                
                this.statistics.ErrorsNum++;
                throw;
            }
        }

        /// <summary>
        /// Zařadí fotografie daného auta do fronty pro stažení a okamžitě smaže fotky
        /// neobsažené v XML datech.
        /// </summary>
        private void QueueOrDeleteCarPhotos(XmlAdverts.Company.CompanyCars.Car xmlCar, CarData car, PhotosQueue queue)
        {
            // Vymažeme fotky, které nejsou vůbec obsaženy v přijatých XML datech
            var existingPhotos = new PhotosDB().ListTc(car.CarID.Value);
            foreach (var photo in existingPhotos)
            {                
                var xmlPhoto = xmlCar.Photos.Items.FirstOrDefault((p) => { return p.FileName == photo.TcName; });
                if (xmlPhoto == null)               
                {
                    Log.LineBegin(String.Format("Mažu fotografii \"{0}\" auta \"{1}\"", photo.TcName, car.ToString()));
                    try
                    {                                             
                        new PhotosDB().Delete(photo.PhotoID.Value);
                        queue.RemovePhoto(photo.TcName);        // zabráníme případnému opětovnému stažení fotky zrušením
                                                                // případného požadavku z fronty
                        Log.LineOK();
                        this.statistics.PhotosDeletedNum++;
                    }
                    catch (Exception ex)
                    {
                        Log.LineErr(ex.Message);
                        Log.NotifyException(ex);
                        this.statistics.ErrorsNum++;
                        continue;
                    }
                }                     
            }

            // Z XML dat zařadíme do fronty pro stažení fotografie, pokud jsou nové nebo aktualizované
            foreach (var xmlPhoto in xmlCar.Photos.Items)
            {
                DateTime? dtLastUpdated = null;
                if (!String.IsNullOrWhiteSpace(xmlPhoto.LastUpdated))
                {
                    DateTime tmpDt;
                    DateTimeFormatInfo datetimeFormat = (DateTimeFormatInfo)CultureInfo.InvariantCulture.DateTimeFormat.Clone();
                    if (DateTime.TryParseExact(xmlPhoto.LastUpdated, "yyyy-MM-dd HH:mm:ss", datetimeFormat, DateTimeStyles.None, out tmpDt))
                        dtLastUpdated = tmpDt;
                    else                    
                        throw new BadTipCarsData(String.Format("Atribut \"datum\" elementu photo s názvem \"{0}\" vozu \"{1}\" obsahuje neočekávaný formát data a času.", xmlPhoto.FileName, xmlCar.ToString()));                    

                    var photo = existingPhotos.FirstOrDefault(p => p.TcName == xmlPhoto.FileName);
                    var request = new PhotosQueue.Requests.Request()
                    {
                        FileName = xmlPhoto.FileName,
                        IsDefault = xmlPhoto.IsDefault == "1",
                        CarID = car.CarID.Value,
                        CarTitle = car.ToString(),
                        LastUpdated = dtLastUpdated.Value
                    };

                    if (photo != null)
                    {
                        if (dtLastUpdated > photo.TcLastUpdated)
                        {
                            request.photoId = photo.PhotoID;
                            queue.PushRequest(request);
                        }
                    }
                    else                   
                        queue.PushRequest(request);                    
                }
            }
        }        

        private void ImportPhotos(PhotosQueue queue)
        {            
            Log.SectionBegin("Zahajuji vkládání a aktualizaci fotografií...");
            try
            {
                foreach (var request in queue.PopRequests())
                {
                    try
                    {
                        if (request.photoId != null)
                        {
                            Log.LineBegin(String.Format("Aktualizuji fotografii \"{0}\" auta \"{1}\"", request.FileName, request.CarTitle));
                            var photo = new PhotosDB().Get(request.photoId.Value);
                            this.UpdatePhoto(request.FileName, photo, request.LastUpdated);
                        }
                        else
                        {
                            Log.LineBegin(String.Format("Vkládám fotografii \"{0}\" auta \"{1}\"", request.FileName, request.CarTitle));
                            this.InsertPhoto(request.FileName, request.IsDefault, request.CarID, request.LastUpdated);
                        }

                        Log.LineOK();                        
                        queue.RemovePhoto(request.FileName);
                    }
                    catch (Exception ex)
                    {                        
                        Log.LineErr(ex.Message);
                        if (!(ex is DownloadImageException))
                            Log.NotifyException(ex);
                        this.statistics.ErrorsNum++;
                    }
                }                

                Log.SectionEnd("Vkládání a aktualizace fotografií dokončeno...");
            }
            catch (Exception ex)
            {
                Log.SectionEnd("Vkládání a aktualizace fotografií dokončeno s chybou: " + ex.Message);
                Log.NotifyException(ex);
                this.statistics.ErrorsNum++;
            }
        }

        /// <summary>
        /// Načte místní auto.
        /// </summary>        
        private CarData LoadCar(string tcID, int dealerID)
        {
            return new CarsDB().GetByTcID(tcID, dealerID);            
        }

        /// <summary>
        /// Aktualizuje místní auto car podle přijatých dat xmlCar pomocí číselníků matchLists.
        /// </summary>
        private void UpdateCar(CarData localCar, XmlAdverts.Company.CompanyCars.Car remoteCar, int dealerID, MatchLists matchLists)
        {
            this.CopyCarData(remoteCar, localCar, dealerID, matchLists);
            new CarsDB().Update(localCar);
            this.statistics.CarsUpdatedNum++;
        }

        /// <summary>
        /// Vytvoří nové místní auto podle přijatých dat xmlCar pomocí číselníků matchLists pro daného dealera.
        /// </summary>
        private int InsertCar(XmlAdverts.Company.CompanyCars.Car remoteCar, int dealerID, MatchLists matchLists)
        {
            CarData car = new CarData();
            car.DealerID = dealerID;
            this.CopyCarData(remoteCar, car, dealerID, matchLists);
            var res = new CarsDB().Insert(car);
            this.statistics.CarsInsertedNum++;
            return res;
        }

        /// <summary>
        /// Sestaví Uri pro stažení fotky.
        /// </summary>
        /// <returns></returns>
        private Uri CreatePhotoURI(string photoName)
        {
            NameValueCollection urlParams = new NameValueCollection();
            urlParams.Add("R", Configuration.RegistrationCode);
            urlParams.Add("F", photoName);

            var uriBuilder = new UriBuilder();
            uriBuilder.Host = Configuration.XMLUriHost;
            uriBuilder.Path = Configuration.XMLUriPhoto;
            uriBuilder.Query = urlParams.ToQueryString();

            return uriBuilder.Uri;
        }

        /// <summary>
        /// Aktualizuje fotku photo novými daty z remotePhoto.
        /// </summary>        
        private void UpdatePhoto(string xmlPhotoFileName, global::Photo localPhoto, DateTime lastUpdated)
        {           
            Image image = this.DownloadPhoto(xmlPhotoFileName);
            new PhotosDB().UpdateTc(localPhoto.PhotoID.Value, lastUpdated, image);         
            this.statistics.PhotosUpdatedNum++;         
        }

        /// <summary>
        /// Vloží nové foto k vozu carID.
        /// </summary>        
        private void InsertPhoto(string xmlPhotoFileName, bool IsDefaultPhoto, int carID, DateTime lastUpdated)
        {                              
            Image image = this.DownloadPhoto(xmlPhotoFileName);

            global::Photo newPhoto = new global::Photo();
            newPhoto.ObjID = carID;
            newPhoto.ObjCat = PhotoCategory.CAR;
            newPhoto.Primary = IsDefaultPhoto;
            newPhoto.TcLastUpdated = lastUpdated;
            newPhoto.TcName = xmlPhotoFileName;
            new PhotosDB().Insert(newPhoto, image);
            this.statistics.PhotosInsertedNum++;                
        }

        /// <summary>
        /// Vymaže fotografie photos.
        /// </summary>        
        private void DeletePhotos(IEnumerable<global::Photo> photos, Dictionary<int, XmlAdverts.Company.CompanyCars.Car> carsDict)
        {
            foreach (var photo in photos)
            {
                try
                {
                    string carName = String.Format("id={0}", photo.ObjID);
                    if (carsDict.ContainsKey(photo.ObjID.Value))
                        carName = carsDict[photo.ObjID.Value].ToString();

                    Log.LineBegin(String.Format("Mažu fotografii \"{0}\" auta \"{1}\"", photo.TcName, carName));
                    new PhotosDB().Delete(photo.PhotoID.Value);
                    Log.LineOK();
                    this.statistics.PhotosDeletedNum++;
                }
                catch (Exception ex)
                {
                    Log.LineErr(ex.Message);
                    Log.NotifyException(ex);
                    this.statistics.ErrorsNum++;
                    continue;
                }
            }
        }

        /// <summary>
        /// Stáhne obrázek daného názvu.
        /// </summary>        
        private Image DownloadPhoto(string photoName)
        {
            string imagePath = null;
            try
            {
                if (Configuration.XMLUriHost != "file")
                {
                    NameValueCollection urlParams = new NameValueCollection();
                    urlParams.Add("R", Configuration.RegistrationCode);
                    urlParams.Add("F", photoName);

                    var uriBuilder = new UriBuilder();
                    uriBuilder.Host = Configuration.XMLUriHost;
                    uriBuilder.Path = Configuration.XMLUriPhoto;
                    uriBuilder.Query = urlParams.ToQueryString();
                    imagePath = uriBuilder.ToString();

                    return Utils.DownloadImage(uriBuilder.Uri);
                }
                else
                {
                    imagePath = Configuration.XMLUriPhoto;
                    return Image.FromFile(imagePath);
                }
               
            }
            catch (Exception ex)
            {
                throw new DownloadImageException(imagePath, ex.InnerException);
            }
        }

        /// <summary>
        /// Kopíruje XML data do lokální třídy auta.
        /// </summary>        
        private void CopyCarData(XmlAdverts.Company.CompanyCars.Car xmlCar, CarData car, int dealerID, MatchLists matchLists)
        {
            // Kontrola elementu custom_car_id
            Action<string> ValidateClientID = (elementValue) =>
            {
                if (String.IsNullOrWhiteSpace(elementValue))                
                    throw new BadTipCarsData(String.Format("Nelze importovat vůz \"{0}\", element \"custom_car_id\" je prázdný.", xmlCar.ToString()));                
            };

            // Kontrola povinných XML elementů
            Action<string, string, string> ValidateNoEmptyElement = (elementName, elementValue, carIdentifier) =>
            {
                if (String.IsNullOrWhiteSpace(elementValue))
                {
                   
                    string message = String.Format(
                        "Nelze importovat vůz \"{0}\", element \"{1}\" je prázdný.",
                        carIdentifier,
                        elementName);                                   

                    throw new BadTipCarsData(message);
                }
            };

            // Kontrola platného čísla
            Func<string, string, string, int> ValidateNumber = (elementName, elementValue, carIdentifier) =>
            {
                if (String.IsNullOrWhiteSpace(elementValue))
                {                    
                  
                    string message = String.Format(
                            "Nelze importovat vůz \"{0}\", element \"{1}\" je prázdný.",
                            carIdentifier,
                            elementName);                 

                    throw new BadTipCarsData(message);
                }

                int result;
                if (!Int32.TryParse(elementValue, out result))                
                    throw new BadTipCarsData(String.Format(
                        "Nelze importovat vůz \"{0}\", element \"{1}\" není platné celé číslo.", 
                        carIdentifier, 
                        elementName));

                return result;
            };

            // Kontrola platné cenové položky
            Func<string, string, string, decimal> ValidateDecimal = (elementName, elementValue, carIdentifier) =>
            {
                if (String.IsNullOrWhiteSpace(elementValue))
                {

                    string message = String.Format(
                            "Nelze importovat vůz \"{0}\", element \"{1}\" je prázdný.",
                            carIdentifier,
                            elementName);

                    throw new BadTipCarsData(message);
                }

                try
                {
                    NumberFormatInfo numberFormat = (NumberFormatInfo)CultureInfo.InvariantCulture.NumberFormat.Clone();
                    numberFormat.NumberDecimalSeparator = ",";
                    return Decimal.Parse(elementValue, NumberStyles.Currency, numberFormat);
                }
                catch (FormatException ex)
                {
                    throw new BadTipCarsData(
                        String.Format(
                            "Nelze importovat vůz \"{0}\", element \"{1}\" není platná cena.",
                            carIdentifier,
                            elementName),
                        ex);
                }             
            };    


            // Validace XML dat
            ValidateNoEmptyElement("custom_car_id", xmlCar.CarID, null);
            ValidateNoEmptyElement("manufacturer_klic", xmlCar.ManufacturerKey, xmlCar.ToString());
            ValidateNoEmptyElement("model_klic", xmlCar.ModelKey, xmlCar.ToString());
            ValidateNoEmptyElement("kind_klic", xmlCar.PurposeKey, xmlCar.ToString());
            ValidateNoEmptyElement("body_klic", xmlCar.BodyKey, xmlCar.ToString());
            ValidateNoEmptyElement("fuel_klic", xmlCar.FuelKey, xmlCar.ToString());
            int engineCapacity = ValidateNumber("engine_volume", xmlCar.EngineCapacity, xmlCar.ToString());
            decimal price = ValidateDecimal("price_end", xmlCar.Price, xmlCar.ToString());

            // určit nepovinnou hodnotu výkonu
            int? enginePower = null;
            if (!String.IsNullOrWhiteSpace(xmlCar.EnginePower) && !String.IsNullOrWhiteSpace(xmlCar.EnginePowerUnitKey))
            {            
                int tmpPowerVal;
                if (Int32.TryParse(xmlCar.EnginePower, out tmpPowerVal))
                {
                    if (xmlCar.EnginePowerUnitKey.Trim() == "A")
                        enginePower = tmpPowerVal;
                    else if (xmlCar.EnginePowerUnitKey.Trim() == "B")
                        enginePower = Convert.ToInt32(Math.Round(tmpPowerVal * 0.745699872));                 
                }
            }

            // Spárování přijatých dat s hodnotami z číselníků            
            var supplier = matchLists.Suppliers.FirstOrDefault( 
                s =>                    
                    s.TcCodes.Split(',').Contains(xmlCar.ManufacturerKey));

            if (supplier == null)
                throw new CannotMatchCar(
                    String.Format(
                        "Nelze přiřadit vůz \"{0}\", protože kód jeho značky \"{1}\" není nastaven v žádném záznamu místního číselníku značek.",
                        xmlCar.ToString(),
                        xmlCar.ManufacturerKey));

            var model = matchLists.Models.FirstOrDefault(
                m =>
                    m.SuppID == supplier.SuppID &&
                    m.TcCodes.Split(',').Contains(xmlCar.ModelKey));

            if (model == null)
                throw new CannotMatchCar(
                    String.Format(
                        "Nelze přiřadit vůz \"{0}\", protože kód jeho modelu \"{1}\" není nastaven v žádném záznamu místního číselníku modelů značek.",
                        xmlCar.ToString(),
                        xmlCar.ModelKey));

            // Kód karoesrie se přiřazuje podle kombinace Druhu vozidla a karosérie. Kombinace je
            // vyjádřena jako dvojice oddělené pomlčkou, např. A-c, B-t, apod. V případě kódu A-a (hatchback) se navíc podle počtu dveří rozeznává HT3 a HT5            
            var body = matchLists.Bodies.FirstOrDefault(                
                b =>
                    b.TcCodes.Split(',').Contains(xmlCar.PurposeKey + "-" + xmlCar.BodyKey)     // podmínka na záznam s existencí daného kódu
                    && (xmlCar.PurposeKey + "-" + xmlCar.BodyKey != "A-a"                       // pokud se jedná o hatchback (HT3 nebo HT5)                         
                            || xmlCar.DoorsNum == "3" &&  b.BodyStyle == "HT3"                  //      ...a počet dveří je 3, vrať true pro HT3    
                            || xmlCar.DoorsNum != "3" &&  b.BodyStyle == "HT5"));               //      ...jinak vrať true pro HT5

            if (body == null)
                throw new CannotMatchCar(
                    String.Format(
                        "Nelze přiřadit vůz \"{0}\", protože kód jeho druhu \"{1}\" a kód jeho karosérie \"{2}\" není nastaven v žádném záznamu místního číselníku karosérií.",
                        xmlCar.ToString(),
                        xmlCar.PurposeKey,
                        xmlCar.BodyKey));

            var vmodel = matchLists.VModels.FirstOrDefault(
                vm =>
                    vm.SuppID == supplier.SuppID && vm.ModelID == model.ModelID && vm.BodyStyle.ToString() == body.BodyStyle);

            if (vmodel == null)
                throw new CannotMatchCar(
                String.Format(
                    "Nelze přiřadit vůz \"{0}\", protože neexistuje odpovídající místní virtuální model [značka=\"{1}\", model=\"{2}\", karosérie=\"{3}\"].",
                    xmlCar.ToString(),
                    supplier.Title,
                    model.Title,
                    body.Title));            

            var fuel = matchLists.Fuels.FirstOrDefault(
                f =>                     
                    f.TcCodes.Split(',').Contains(xmlCar.FuelKey));

            if (fuel == null)
                throw new CannotMatchCar(
                    String.Format(
                        "Nelze přiřadit vůz \"{0}\", protože kód jeho paliva \"{1}\" není nastaven v žádném záznamu místního číselníku paliv.",
                        xmlCar.ToString(),
                        xmlCar.FuelKey));

            ListColor color = null;
            if (!String.IsNullOrWhiteSpace(xmlCar.ColorKey))
            {
                color = matchLists.Colors.FirstOrDefault(
                c =>
                    c.TcCodes.Split(',').Contains(xmlCar.ColorKey));
            }

            // Nastavení povinných vlastností

            car.SuppID = supplier.SuppID;
            car.ModelID = model.ModelID;
            car.VModelID = vmodel.VModelID;
            car.DealerID = dealerID;            
            car.BodyStyle = (VModelBodyStyle)Enum.Parse(typeof(VModelBodyStyle), body.BodyStyle);
            car.Capacity = engineCapacity;
            car.Fuel = (CarFuel)Enum.Parse(typeof(CarFuel), fuel.Fuel);
            car.Price = Math.Round(price * (decimal)1.21);

            // Nastavení nepovinných vlastností           
            car.TcID = xmlCar.CarID;              
            car.VinID = "TC" + xmlCar.CarID;
            car.Params = xmlCar.Equipment.ToString();

            if (enginePower != null)
                car.Power = enginePower;

            if (color != null)
                car.ColorID = color.ColorID;

            if (!String.IsNullOrWhiteSpace(xmlCar.TypeInfo))
                car.Title = xmlCar.TypeInfo;

            if (xmlCar.IsNoPublic.Trim() == "B" || (xmlCar.IsNoPublic.IndexOf("X") != -1 && xmlCar.IsNoPublic.IndexOf("A") != -1))
                car.Published = false;
            else
                car.Published = true;

            car.Available = CarAvailable.SKL;
            if (!String.IsNullOrWhiteSpace(xmlCar.ConditionKey))
            {
                if (xmlCar.ConditionKey.Trim() == "P")
                    car.Available = CarAvailable.PRE;
            }
                        
        }

        /// <summary>
        /// Nastaví výbavu (parametry) lokálního vozu carId podle xmlCar s využitím číselníku parametrů listParams.
        /// </summary>        
        private void SaveCarParams(XmlAdverts.Company.CompanyCars.Car remoteCar, int carId, IEnumerable<ListParam> listParams)
        {
            // Přiřazení parametrů
            List<CarsParam> carParams = new List<CarsParam>();
            foreach (var equipKey in remoteCar.Equipment.Keys)
            {
                var equipment = listParams.FirstOrDefault(
                   e =>
                       e.TcCodes.Split(',').Contains(equipKey));               

                if (equipment != null)
                {
                    if (carParams.Exists(i => i.ParamID == equipment.ParamID))
                        continue;

                    CarsParam cp = new CarsParam();
                    cp.CarID = carId;
                    cp.ParamID = equipment.ParamID;
                    cp.ValueBIT = true;
                    carParams.Add(cp);
                }
            }

            new CarsParamsDB().Set(carParams, carId); 
        }

        /// <summary>
        /// Smaže auta, která nejsou přítomná v xmlCars.
        /// </summary>       
        private void DeleteOthersCars(IEnumerable<XmlAdverts.Company.CompanyCars.Car> xmlCars, int dealerId)
        {                       
            var db = new CarsDB();

            var existingCars = db.FindByDealer(dealerId)
                .Where((c) => { return !String.IsNullOrEmpty(c.TcID); })
                .Select((c) => c.TcID);

            var advertsCars = xmlCars
                .Select((c) => c.CarID);

            var carsToDelete = existingCars.Except(advertsCars);

            foreach (string tcId in carsToDelete)
            {
                var car = db.GetByTcID(tcId, dealerId);
                if (car == null)
                    continue;

                try
                {
                    Log.LineBegin(String.Format("Mažu z databáze auto \"{0}\"", car.ToString()));
                    db.Delete(car.CarID.Value);
                    this.statistics.CarsDeletedNum++;
                    Log.LineOK();

                }
                catch (Exception ex)
                {
                    Log.NotifyException(ex);
                    Log.LineErr(ex.Message);
                }
            }         
        }
    }
}
