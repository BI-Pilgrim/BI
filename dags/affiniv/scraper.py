from datetime import datetime
import json
from pytz import timezone, utc
import requests
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Union, Any

import requests.structures

 
class ReportJobItem(BaseModel):
    id: Optional[str] = None
    surveyId: Optional[int] = None
    companyId: Optional[str] = None
    requestedByUserId: Optional[str] = None
    jobStartTime: Optional[str] = None
    jobEndTime: Optional[str] = None
    periodStartTime: Optional[str] = None
    periodEndTime: Optional[str] = None
    s3ReportLocation: Optional[str] = None
    reportType: Optional[str] = None

class Company(BaseModel):
    id: Optional[str] = None
    createdDate: Optional[str] = None
    modifiedDate: Optional[str] = None
    companyName: Optional[str] = None
    companyURLCode: Optional[str] = None
    websiteURL: Optional[str] = None
    timeZone: Optional[str] = None
    addressLine1: Optional[str] = None
    addressLine2: Optional[str] = None
    city: Optional[str] = None
    countryCode: Optional[str] = None
    companyMainLogoLocationS3: Optional[str] = None
    shopifyStore: Optional[bool] = None
    shopifyStoreToken: Optional[str] = None
    shopifyStoreDomain: Optional[str] = None
    shopifyStoreUrl: Optional[str] = None
    showDefaultShopifyView: Optional[bool] = None
    shopifyWebhookIdForOrderCreation: Optional[str] = None
    shopifyWebhookIdForOrderCancellation: Optional[str] = None
    shopifyWebhookIdForOrderFulfillment: Optional[str] = None
    shopifyCustomAppAPIKey: Optional[str] = None
    shopifyCustomAppClientSecret: Optional[str] = None
    shopifyAppName: Optional[str] = None
    subscriptionWarningType: Optional[str] = None
    subscriptionWarningText: Optional[str] = None
    displayName: Optional[str] = None
    leadFullName: Optional[str] = None
    leadEmail: Optional[str] = None
    leadPhone: Optional[str] = None
    last30DayUsersOnCreatedDate: Optional[str] = None
    customEventProcessingForShopify: Optional[bool] = None
    gdprEnabled: Optional[bool] = None
    shopifyNewOrderCreationPaused: Optional[bool] = None
    isSurveySendingPaused: Optional[bool] = None
    primaryEmail: Optional[str] = None
    primaryMailPassword: Optional[str] = None
    primaryFullName: Optional[str] = None
    primaryPhone: Optional[str] = None
    defaultShopifySurveyId: Optional[str] = None
    emailTemplateSettingsCreated: Optional[bool] = None
    surveyDistSettingForEmailCreated: Optional[bool] = None

class EfmUser(BaseModel):
    id: Optional[str] = None
    createdDate: Optional[str] = None
    modifiedDate: Optional[str] = None
    email: Optional[str] = None
    fullName: Optional[str] = None
    phone: Optional[str] = None
    password: Optional[str] = None

class AffinivLoginData(BaseModel):
    id: Optional[str] = None
    createdDate: Optional[str] = None
    modifiedDate: Optional[str] = None
    lastAccessTime: Optional[str] = None
    joinedDate: Optional[str] = None
    officialRole: Optional[str] = None
    company: Optional[Company] = None
    efmUser: Optional[EfmUser] = None
    role: Optional[str] = None
    isApiUser: Optional[bool] = None
    token: Optional[str] = None
    tokenExpiryDate: Optional[str] = None
    userInvitationId: Union[str, None, int] = None
    joined: Optional[bool] = None
    ftu: Optional[bool] = None

class SurveyTheme(BaseModel):
    id: Optional[str] = None
    companyId: Optional[str] = None
    backgroundImg: Optional[str] = None
    font: Optional[str] = None
    questionColor: Optional[str] = None
    answersColor: Optional[str] = None
    buttonsColor: Optional[str] = None
    buttonTextColor: Optional[str] = None
    backgroundColor: Optional[str] = None
    logoUrl: Optional[str] = None
    logoSizeInPx: Optional[int] = None
    preCreated: Optional[bool] = None
    createdDate: Optional[str] = None
    modifiedDate: Optional[str] = None

class ScaleDetails(BaseModel):
    scaleStartsWith: Optional[int] = None
    scaleEndsWith: Optional[int] = None
    scaleLowLabel: Optional[str] = None
    scaleHighLabel: Optional[str] = None
    scaleDisplayType: Optional[str] = None

class ShowOptionsForStandardSurveys(BaseModel):
    showToPromoterNPS: Optional[bool] = None
    showToDetractorNPS: Optional[bool] = None
    showToPassiveNPS: Optional[bool] = None
    showToSatCSAT: Optional[bool] = None
    showToNeutralCSAT: Optional[bool] = None
    showToDisSatCSAT: Optional[bool] = None
    showToAgreeCES: Optional[bool] = None
    showToNeutralCES: Optional[bool] = None
    showToDisagreeCES: Optional[bool] = None

class Question(BaseModel):
    id: Optional[str] = None
    createdDate: Optional[str] = None
    modifiedDate: Optional[str] = None
    questionOptions: Optional[list] = None
    logicRules: Optional[list] = None
    questionType: Optional[str] = None
    mainQuestionText: Optional[str] = None
    description: Optional[str] = None
    questionImage: Optional[str] = None
    required: Optional[bool] = None
    multipleOptionSelectable: Optional[bool] = None
    showOtherOption: Optional[bool] = None
    otherOptionTextRequired: Optional[bool] = None
    randomizeOptions: Optional[bool] = None
    hidden: Optional[bool] = None
    orderWithinSurveyTemplate: Optional[int] = None
    annotation: Optional[str] = None
    scaleDetails: Optional[ScaleDetails] = None
    showOptionsForStandardSurveys: Optional[ShowOptionsForStandardSurveys] = None
    minCharsForOtherOption: Optional[int] = None
    errMsgForOtherOptionBoxIfMinCharsNotEntered: Optional[str] = None
    customQuestionProcessorClass: Optional[str] = None
    isColoredNumericScale: Optional[bool] = None
    isPictureOptionsForChoiceQuestion: Optional[bool] = None

class SurveyTemplate(BaseModel):
    id: Optional[str] = None
    survey: Optional[int] = None
    createdDate: Optional[str] = None
    modifiedDate: Optional[str] = None
    backgroundImg: Optional[str] = None
    startPage: Optional[str] = None
    thankYouPages: Optional[list] = None
    questions: Optional[list[Question]] = None
    templateType: Optional[str] = None
    copySetting: Optional[str] = None
    preCreated: Optional[bool] = None
    surveyTheme: Optional[SurveyTheme] = None
    firstQuestion: Optional[str] = None
    captureFirstQResponse: Optional[bool] = None
    captureAllQResponse: Optional[bool] = None

class Survey(BaseModel):
    id: Optional[str] = None
    createdDate: Optional[str] = None
    modifiedDate: Optional[str] = None
    company: Optional[Company] = None
    surveyType: Optional[str] = None
    surveyName: Optional[str] = None
    surveyTemplate: Optional[SurveyTemplate] = None
    segmentToSurvey: Optional[str] = None
    sampleSurveyTemplate: Optional[str] = None
    numResponses30Days: Optional[int] = None
    numResponsesAllTime: Optional[int] = None
    scoreForTurnKeySurvey: Optional[int] = None
    dashboardResult: Optional[str] = None
    defaultSurveyForCompany: Optional[bool] = None
    showPoweredByAffinivLink: Optional[bool] = None
    hideBackButton: Optional[bool] = None
    turnKeySurvey: Optional[bool] = None
    npsScoreLabels: Optional[dict] = None
    csatScoreLabels: Optional[dict] = None
    cesScoreLabels: Optional[dict] = None
    surveyTypeDesc: Optional[str] = None
    draftSurvey: Optional[bool] = None

class SurveyListResponse(BaseModel):
    surveyList: List[Survey] = None
    company: Optional[Company] = None

BASE_URL = "https://app.affiniv.com/api"

BASE_HEADERS = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0",
                "Accept":"application/json, text/plain, */*",
                "Accept-Language":"en-US,en;q=0.5",
                "Accept-Encoding":"gzip, deflate, br, zstd",
                "Content-Type":"application/json;charset=utf-8",
                "Origin:https":"//app.affiniv.com",
                "Alt-Used":"app.affiniv.com",
                "Connection":"keep-alive",
                "Referer":"https://app.affiniv.com/login",
                "Sec-Fetch-Dest":"empty",
                "Sec-Fetch-Mode":"cors",
                "Sec-Fetch-Site":"same-origin",
                "DNT":"1",
                "Sec-GPC":"1",
                "Priority":"u=0",
                "Pragma":"no-cache",
                "Cache-Control":"no-cache",
                "TE":"trailers"}

class LoginError(ValueError):
    pass


class LoginInitializer(BaseModel):
    session_headers:Dict
    token: str
    company_id:str

class AffinivScraper:
    
    def __init__(self, username:str, password:str, tokenInitializer:LoginInitializer=None):
        self.session = requests.Session()
        if tokenInitializer:
            self.token = tokenInitializer.token
            self.session.headers.update(tokenInitializer.session_headers)
            self.login_data = AffinivLoginData(**{"token": tokenInitializer.token, "company": {"id": tokenInitializer.company_id}})
        else: self.login_data = self.login(username, password)
    
    def login(self, username:str, password:str)->AffinivLoginData:
        url = f"{BASE_URL}/pub/v1/token"
        resp = self.session.post(url, json={"email": username, "password": password})
        if resp.ok:
            login_data = AffinivLoginData(**resp.json())
            self.token = login_data.token
            self.token = "9b262a43-4652-416d-ba33-160cbf27a46a"
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            return login_data
        raise LoginError(f"Failed to login: {resp.text}")
    
    def get_survey_list(self, company_id:str)->SurveyListResponse:
        url = f"{BASE_URL}/v1/companies/{company_id}/surveys/details"
        resp = self.session.get(url)
        if resp.ok:
            return SurveyListResponse(**resp.json())
        raise ValueError(f"Failed to get survey details: {resp.text}")
    
    def get_survey_results_for_period(self, survey_id:str, start_date:datetime, end_date:datetime):
        start_date_str:str = AffinivScraper.get_time_str(self.start_date)
        end_date_str:str = AffinivScraper.get_time_str(self.end_date)
        url = f"{BASE_URL}/v1/surveys/{survey_id}/results/dashboard"
        resp = self.session.post(url, params={"startDate":start_date_str, "endDate":end_date_str})
        if resp.ok:
            return resp.json()
        raise ValueError(f"Failed to get survey results: {resp.text}")

    def get_survey_stats_for_period(self, survey_id:str, start_date:datetime, end_date:datetime):
        start_date_str:str = AffinivScraper.get_time_str(start_date)
        end_date_str:str = AffinivScraper.get_time_str(end_date)
        url = f"{BASE_URL}/v1/surveys/{survey_id}/results/stats/forPeriod"
        resp = self.session.get(url, params={"startDate":start_date_str, "endDate":end_date_str})
        if resp.ok:
            return resp.json()
        raise ValueError(f"Failed to get survey results: {resp.text}")
    
    def get_survey_summary_for_period(self, survey_id:str, start_date:datetime, end_date:datetime):
        start_date_str:str = AffinivScraper.get_time_str(start_date)
        end_date_str:str = AffinivScraper.get_time_str(end_date)
        url = f"{BASE_URL}/v1/surveys/{survey_id}/results/summary"
        resp = self.session.post(url, params={"startDate":start_date_str, "endDate":end_date_str})
        if resp.ok:
            return resp.json()
        raise ValueError(f"Failed to get survey results: {resp.text}")
    
    def get_survey_tags_for_period(self, survey_id:str, start_date:datetime, end_date:datetime):
        start_date_str:str = AffinivScraper.get_time_str(start_date)
        end_date_str:str = AffinivScraper.get_time_str(end_date)
        url = f"{BASE_URL}/v1/surveys/{survey_id}/results/tags/forPeriod"
        resp = self.session.post(url, params={"startDate":start_date_str, "endDate":end_date_str})
        if resp.ok:
            return resp.json()
        raise ValueError(f"Failed to get survey results: {resp.text}")
    
    def get_report_jobs(self, survey_id:str, start_date:Optional[datetime]=None, end_date:Optional[datetime]=None)->List[ReportJobItem]:
        start_date_str:str = AffinivScraper.get_time_str(start_date)
        end_date_str:str = AffinivScraper.get_time_str(end_date)
        url = f"{BASE_URL}/v1/surveys/{survey_id}/report-jobs"
        resp = self.session.get(url, params={"startDate":start_date_str, "endDate":end_date_str})
        if resp.ok:
            return [ReportJobItem(**x) for x in resp.json()]
        raise ValueError(f"Failed to get report status: {resp.text}")
    
    def trigger_report(self, survey_id:str, start_date:datetime, end_date:datetime):
        start_date_str:str = AffinivScraper.get_time_str(start_date)
        end_date_str:str = AffinivScraper.get_time_str(end_date)
        url = f"{BASE_URL}/v1/surveys/{survey_id}/results/dashboard/report/apimode"
        print(url, start_date_str, end_date_str)
        try:
            resp = self.session.post(url, params={"startDate":start_date_str, "endDate":end_date_str})
            if resp.ok:
                return ReportJobItem(**resp.json())
        except Exception as e:
            # This will timeout, so we need to catch the exception
            pass
        # raise ValueError(f"Failed to trigger report: {resp.text}")
    
    @staticmethod
    def get_time_str(date_time:datetime)->str:
        local_tz = timezone("Asia/Kolkata")
        date_time = date_time.replace(tzinfo=local_tz)
        return date_time.astimezone(utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    


        
    

        