// @ts-nocheck
"use client";

import { useState, useRef, useEffect } from "react";

/**
 * LOE Generator — Engagement Letter Generator for NPSA.
 * This is the ONE FILE your client edits. Templates, pricing, and form logic live here.
 * Push changes to GitHub when ready. Hosted on the same frontend (Vercel).
 */

// ─── PRICING TABLES ───────────────────────────────────────────────────────────
const PRICING = {
  "pre-only": {
    label: "Pre-Award Only",
    tiers: {
      undiscounted: { 1: 4000, 2: 8000, 3: 12000 },
      discounted:   { 1: 3500,  2: 7000, 3: 10500 },
      max:          { 1: 8000,  2: 13500, 3: 18000 },
    },
    contingent: null,
  },
  "partial-contingency": {
    label: "Pre-Award + Partial Contingency",
    tiers: {
      undiscounted: { upfront: { 1: 3000, 2: 6000, 3: 9000 }, contingent: { 1: 11000, 2: 16000, 3: 22000 } },
      discounted:   { upfront: { 1: 2000, 2: 4000, 3: 6000 }, contingent: { 1: 9000,  2: 14000, 3: 19000 } },
      max:          { upfront: { 1: 0,    2: 0,    3: 0    }, contingent: { 1: 11000, 2: 18000, 3: 25000 } },
    },
  },
  "inh-pre-only": {
    label: "Pre-Award Only (In-House)",
    tiers: {
      undiscounted: { 1: 11000, 2: 18000, 3: 25000 },
      discounted:   { 1: 9500,  2: 15000, 3: 20500 },
      max:          { 1: 8000,  2: 13500, 3: 18000 },
    },
    contingent: null,
  },
  "inh-partial-contingency": {
    label: "Pre-Award + Partial Contingency (In-House)",
    tiers: {
      undiscounted: { upfront: { 1: 3000, 2: 6000, 3: 9000 }, contingent: { 1: 11000, 2: 16000, 3: 22000 } },
      discounted:   { upfront: { 1: 2000, 2: 4000, 3: 6000 }, contingent: { 1: 9000,  2: 14000, 3: 19000 } },
      max:          { upfront: { 1: 0,    2: 0,    3: 0    }, contingent: { 1: 11000, 2: 18000, 3: 25000 } },
    },
  },
};

const TIER_LABELS = { undiscounted: "Undiscounted", discounted: "Discounted", max: "Max Discount", custom: "Custom" };
const fmt = (n) => n === 0 ? "$0" : `$${Number(n).toLocaleString()}`;

// ─── DEFAULT TEMPLATE SECTIONS ────────────────────────────────────────────────
const DEFAULT_PRE = [
  { id:"pre_intro", title:"Introduction", roman:"",
    content:`Lynde Consulting LLC, DBA Nonprofit Security Advisors ("NPSA") proposes to provide [CLIENT_NAME] ("CLIENT") with pre-award consulting, grant writing, and submission services. Post-award grant management and implementation services, if any, will be governed by a separate written agreement.

The project will encompass various potential security improvements which may include but may not be limited to various services and equipment such as (1) physical security equipment; (2) surveillance and monitoring equipment and services; (3) communications systems; (4) cybersecurity improvements; (5) security training drills; and (6) contracted security personnel.

This engagement specifically applies to the [GRANT_YEAR] Federal Nonprofit Security Grant Program ("NSGP"). All commitments made below relate to services and projects associated with this program.`},
  { id:"pre_scope", title:"Scope of Work", roman:"I.",
    subsections:[{ id:"pre_scope_pre", title:"A. Pre-Award Consulting",
      content:`1. NPSA will complete an initial fact-finding meeting with CLIENT to determine the project goals and to obtain additional preliminary information.

2. NPSA will coordinate with its network of grant specialists and grant writers to determine whether CLIENT is likely eligible for the anticipated [GRANT_YEAR] NSGP.

3. NPSA already believes, but does not guarantee, that CLIENT is likely eligible for one application of the [GRANT_YEAR] NSGP which includes funding for target hardening and other physical security enhancements. The total maximum grant award is $[MAX_AWARD] per awarded location.
   (a) Under current program guidelines, CLIENT may submit one application per physical address, for up to [NUM_LOCATIONS] distinct location(s). Each location is scored independently and may be awarded the maximum funding amount.
   (b) This engagement includes support for the submission of [NUM_APPLICATIONS] [APPLICATION_PLURAL] for [NUM_LOCATIONS] distinct location(s), at the following address(es):
[LOCATION_LIST]

4. NPSA will identify one or more grant specialists/writers who will provide grant writing and/or consulting related to the grant opportunities, processes, and requirements.
   (a) CLIENT will vet the grant specialist/writers and determine whether to engage in a paid consulting and grant writing contract.
   (b) NPSA receives no referral fees, commissions, or other compensation from any grant specialist/writer.

5. If client elects not to retain the recommended grant writer, Consultant will assist with introductions to an alternative grant writer selected by the client.

6. While NPSA recommends certain grant writers with experience with the NSGP, NPSA does not require CLIENT to choose the grant writer(s) we recommend. However, this engagement is contingent upon CLIENT selecting a professional grant writer with NSGP grant writing experience.

7. If CLIENT chooses to proceed with grant writing services, NPSA will work with CLIENT and grant specialist/grant writer to complete fact finding and obtain preliminary information necessary to proceed.

8. NPSA will assist the grant specialist/writer in arranging for a qualified professional security consultant to be contacted by CLIENT to complete a vulnerability assessment, or will work with the grant specialist/writer to aid CLIENT in completing a self-assessment. In many cases, a professional assessment can be obtained at little or no cost to CLIENT.

9. NPSA will ensure the vulnerability assessment is reviewed with CLIENT and grant specialist/writer and will compare the assessment with CLIENT'S project goals.

10. NPSA will work with CLIENT and the grant specialist/writer to provide program guidance, factual clarification, and administrative coordination consistent with CLIENT'S goals and the vulnerability assessment. Grant application drafting and submission will be performed solely by the grant specialist/writer and/or CLIENT, and not by NPSA.`},
    { id:"pre_scope_post", title:"B. Post Award Consulting and Administrative Support",
      content:`1. Upon notification that CLIENT has been awarded funding under the [GRANT_YEAR] NSGP, NPSA will provide post-award consulting and administrative support services from the date of award announcement through receipt of formal written clearance from the State authorizing CLIENT to begin committing grant funds.

2. NPSA will attend required State and/or federal post-award webinars and review applicable guidance, timelines, policies, and procedures related to CLIENT'S award.

3. NPSA will assist CLIENT with registration and setup in required State grant management systems and will provide guidance regarding administrative documentation, including internal controls questionnaires (ICQ), risk assessments, and other compliance-related submissions required by the State administering agency.

4. NPSA will guide CLIENT through the State budget review, alignment, and allowability process and will assist in confirming that the approved federal application aligns with State-level requirements and funding conditions.

5. NPSA will provide guidance and administrative coordination related to completion and submission of Environmental and Historic Preservation (EHP) documentation, including assistance with required photographs, annotations, equipment specifications, and follow-up communication until approval is issued.

6. NPSA will review grant award documents and coordinate administrative execution of grant agreements and related documentation.

7. If designated by CLIENT as an authorized point of contact, NPSA will receive and review State communications related to the award and will provide CLIENT with summaries, signature pages, and required action items as appropriate.

8. NPSA will attend required trainings related to procurement, reimbursement, and financial compliance and will provide general guidance to CLIENT regarding applicable procurement standards and reimbursement requirements.

9. NPSA will assist CLIENT with registration or coordination with applicable State comptroller, treasury, or financial systems as required for reimbursement processing.

10. NPSA will provide ongoing coordination and communication support between CLIENT and the State administering agency during the pre-clearance period and will identify required action items and deadlines necessary to obtain clearance to begin committing grant funds.

11. These services apply only to the initial post-award compliance phase required before an award recipient is authorized to begin committing grant funds.

12. These services are advisory and administrative in nature and are intended to assist CLIENT in navigating State-specific post-award requirements. Because post-award procedures vary by State and may change annually, specific tasks, documentation requirements, and administrative processes may differ from those listed above. The services described in this section are intended to reflect the typical scope of assistance provided during the initial post-award compliance phase.

13. NPSA's services under this section shall be deemed complete upon CLIENT's receipt of formal written clearance from the State authorizing the commitment of grant funds. At that time, all services described in this engagement shall be fully performed and earned unless otherwise governed by a separate written agreement.

14. Following receipt of such clearance, CLIENT may elect to engage NPSA under a separate written agreement for project management, solution implementation, procurement coordination, reimbursement coordination, documentation support, and grant close-out services.`}]},
  { id:"pre_liability", title:"Limits of Liability", roman:"II.",
    content:`1. CLIENT acknowledges that NPSA is not providing a vulnerability assessment and is simply providing licensed resources which CLIENT will vet. CLIENT is responsible for the final selection of security personnel and will contract with them independently of NPSA. CLIENT agrees NPSA will not be responsible for any recommendations or lack of recommendations made by security personnel.

2. Regardless of whether CLIENT chooses to complete a self-assessment or chooses to contract with a licensed security consultant, CLIENT will not hold NPSA responsible for any security assessments or decisions.

3. CLIENT acknowledges responsibility for choosing any security contractors or solutions providers. CLIENT is responsible for verifying the solutions are implemented satisfactorily and continue to operate satisfactorily. CLIENT will never hold NPSA responsible for any solutions failures.

4. CLIENT acknowledges that NPSA is not responsible for any security breaches or failures, or any harm caused by any bad actor irrespective of NPSA's role in this engagement.

5. With the exception of liability arising out of intentional and malicious acts by NPSA, CLIENT agrees NPSA shall never be held liable for an amount greater than the total fees paid by CLIENT to NPSA as part of this engagement. Furthermore, to the fullest extent possible, CLIENT shall not pursue NPSA for claims or damages that are covered (assuming compliance with the policy terms), in whole or part, by an insurance policy that insures CLIENT, and CLIENT waives any insurer or insured rights of subrogation related to said claim(s). In those instances, in which a loss could or are covered by insurance, CLIENT shall recover solely from insurance benefits or proceeds.

6. CLIENT agrees that in no event shall NPSA be liable for any indirect, incidental, consequential, special, exemplary, or punitive damages, including but not limited to loss of use, loss of revenue, loss of funding, or loss of goodwill, even if advised of the possibility of such damages.`},
  { id:"pre_conf", title:"Confidentiality", roman:"III.",
    content:`1. NPSA acknowledges and agrees that all CLIENT information shared by CLIENT as part of this engagement is owned by CLIENT. These documents are valuable assets of the CLIENT. Except for disclosure required to be made to advance the engagement and information which is a matter of public record, NPSA shall not use any information for the benefit of NPSA or any other person except with express written CLIENT consent.

2. CLIENT acknowledges and agrees that all NPSA information shared by NPSA as part of this engagement is owned by NPSA. This information includes but is not limited to this Engagement Letter, sales and marketing materials, cost information, and invoices. CLIENT agrees not to share this information with any person or organization except for the purposes of advancing this engagement.`},
  { id:"pre_resp", title:"Client Responsibilities", roman:"IV.",
    content:`1. CLIENT will attend all necessary CLIENT meetings whether in person or via web meeting and will provide reasonable advance notice in the event a meeting needs to be rescheduled.

2. CLIENT will provide all necessary information in a timely manner.

3. CLIENT agrees to make every possible effort to complete client-assigned tasks within a timely manner as set forth by the grant writer/NPSA.

4. CLIENT will be responsible for providing access to CLIENT facilities to grant specialists/grant writers, security consultants, solutions providers, NPSA, and any other project stakeholder. CLIENT acknowledges that NPSA will perform all or most services under this engagement remotely. NPSA will not routinely access CLIENT's facilities. Any task requiring on-site presence, including vulnerability assessments or site reviews, will be completed by CLIENT's employees, contractors, law enforcement partners, or other third-party consultants at CLIENT's expense unless otherwise agreed in writing by both parties.

5. CLIENT acknowledges that while NPSA may assist with reviewing vendor proposals, pricing, and project documentation for purposes of grant compliance and administrative coordination, NPSA does not approve, recommend, warrant, or guarantee the performance, availability, quality, or work of any vendor, contractor, consultant, or solutions provider. CLIENT retains sole and final responsibility for all vendor selection decisions.

6. CLIENT agrees to provide written notice of any issues or problems that may arise within 48 hours of uncovering the issue.`},
  { id:"pre_comp", title:"Compensation", roman:"V.", content:"[COMP_BLOCK]" },
  { id:"pre_guar", title:"Guarantees of NPSA", roman:"VI.",
    content:`1. NPSA shall perform duties as agreed to by both parties and shall complete these duties in good faith.

2. CLIENT acknowledges that NPSA does not control federal, state, or third-party funding decisions and that no representations or guarantees are made regarding the approval, amount, or timing of any grant award.

3. [NOFO_CLAUSE]

4. If CLIENT applies for the [GRANT_YEAR] NSGP, the federal government provides a Notice of Funding, and CLIENT is not awarded any grant funding, no refund will be issued for fees collected as part of this engagement, but NPSA will provide the services outlined in this engagement letter for the next available NSGP Opportunity and waive the [UPFRONT_FEE] fee associated with COMPENSATION Section 1 above. In the event this occurs, and CLIENT is awarded grant funding, the fees associated with COMPENSATION Section 1 above will apply and all other provisions of this agreement will apply to the subsequent NSGP Opportunity.

5. If CLIENT elects not to submit an application for the [GRANT_YEAR] NSGP, or if CLIENT fails to provide required information, documentation, approvals, or cooperation necessary to complete the application within required deadlines, no refund shall be issued and all fees paid under this Agreement shall be considered earned.`},
  { id:"pre_cancel", title:"Cancellation", roman:"VII.",
    content:`1. Cancellation. The CLIENT may terminate this Agreement at any time, at will, and in the CLIENT's sole discretion.
   (b) If the CLIENT cancels this Agreement before NPSA has delivered a completed grant application ready for submission, any fees paid for services not yet performed shall be refunded, and any outstanding invoices for completed work shall remain due and payable.
   (c) If the CLIENT cancels this Agreement after NPSA has provided one or more completed grant applications ready for submission, the services under this Agreement shall be deemed fully earned and all fees paid shall be non-refundable.
   (d) If the CLIENT believes NPSA has materially breached this Agreement, the CLIENT must provide written notice to NPSA within forty-eight (48) hours of becoming aware of the alleged breach, and NPSA shall be given a reasonable opportunity to cure the breach prior to termination for cause.`},
  { id:"pre_other", title:"Other Terms and Conditions", roman:"VIII.",
    content:`1. NPSA is an independent contractor performing services for CLIENT and is not an agent or employee of CLIENT for any purpose.

2. CLIENT acknowledges that NPSA provides consulting and administrative services only and is not acting as a contractor, general contractor, installer, vendor, or solutions provider for any security equipment, systems, or services.

3. CLIENT agrees to provide NPSA with such information, necessary and reasonable, to perform the proposed services.

4. CLIENT is solely responsible for selecting, contracting, and compensating any grant writer.

5. NPSA may provide examples of independent grant writers but does not approve, select, negotiate with, or contract on behalf of CLIENT.

6. This Agreement does not obligate CLIENT to engage NPSA for post-award management and administration services.

7. Following execution of a grant award agreement with the State and receipt of required environmental and historical preservation (EHP) approval, CLIENT may select a post-award management and administration provider. CLIENT is free to select any qualified provider. NPSA may provide such services if CLIENT elects to engage NPSA.

8. In the event CLIENT elects to engage NPSA for post-award management and administration services, such services eligible for reimbursement under the grant will commence only after:
   (a) execution of a separate agreement between CLIENT and NPSA,
   (b) CLIENT's receipt of a fully executed grant award agreement from the State, and
   (c) receipt of required EHP approval.

9. This represents the entire proposal of NPSA. Any change to this document must be in writing and agreed upon by an authorized officer of NPSA. This proposal does not become a contract between CLIENT and NPSA until an officer of NPSA has accepted it and a signed copy returned to CLIENT.

10. The parties agree that any and all legal actions will be brought in the County of Winnebago, State of Illinois. To the extent necessary, the parties agree to submit to jurisdiction and waive any and all venue objections.`},
];

// ─── IN-HOUSE PRE-AWARD TEMPLATE ──────────────────────────────────────────────
const DEFAULT_INH = [
  { id:"inh_intro", title:"Introduction", roman:"",
    content:`Lynde Consulting LLC, DBA Nonprofit Security Advisors ("NPSA") proposes to provide [CLIENT_NAME] ("CLIENT") with pre-award consulting, grant writing, and submission services. Post-award grant management and implementation services, if any, will be governed by a separate written agreement.

The project will encompass various potential security improvements which may include but may not be limited to various services and equipment such as (1) physical security equipment; (2) surveillance and monitoring equipment and services; (3) communications systems; (4) cybersecurity improvements; (5) security training drills; and (6) contracted security personnel.

This engagement specifically applies to the [GRANT_YEAR] Federal Nonprofit Security Grant Program ("NSGP"). All commitments made below relate to services and projects associated with this program.`},
  { id:"inh_scope", title:"Scope of Work", roman:"I.",
    subsections:[{ id:"inh_scope_pre", title:"A. Pre-Award Consulting",
      content:`1. NPSA will complete an initial fact-finding meeting with CLIENT to determine the project goals and to obtain additional preliminary information.

2. NPSA will evaluate CLIENT's eligibility for the anticipated [GRANT_YEAR] NSGP.

3. NPSA already believes, but does not guarantee, that CLIENT is likely eligible for one application of the [GRANT_YEAR] NSGP which includes funding for target hardening and other physical security enhancements. The total maximum grant award is $[MAX_AWARD] per awarded location.
   (a) Under current program guidelines, CLIENT may submit one application per physical address, for up to [NUM_LOCATIONS] distinct location(s). Each location is scored independently and may be awarded the maximum funding amount.
   (b) This engagement includes support for the submission of [NUM_APPLICATIONS] [APPLICATION_PLURAL] for [NUM_LOCATIONS] distinct location(s), at the following address(es):
[LOCATION_LIST]

4. NPSA will provide grant writing and application development services directly on behalf of CLIENT, including all drafting, compilation, and submission of the grant application.

5. NPSA will arrange for a qualified professional security consultant to be contacted by CLIENT to complete a vulnerability assessment, or will work with CLIENT to complete a self-assessment. In many cases, a professional assessment can be obtained at little or no cost to CLIENT.

6. NPSA will review the vulnerability assessment with CLIENT and compare it with CLIENT's project goals.

7. NPSA will provide program guidance, factual clarification, and administrative coordination consistent with CLIENT's goals and the vulnerability assessment.

8. NPSA will coordinate all grant application drafting and submission activities and will work directly with CLIENT to finalize and submit the completed application.`},
    { id:"inh_scope_post", title:"B. Post Award Consulting and Administrative Support",
      content:`1. Upon notification that CLIENT has been awarded funding under the [GRANT_YEAR] NSGP, NPSA will provide post-award consulting and administrative support services from the date of award announcement through receipt of formal written clearance from the State authorizing CLIENT to begin committing grant funds.

2. NPSA will attend required State and/or federal post-award webinars and review applicable guidance, timelines, policies, and procedures related to CLIENT'S award.

3. NPSA will assist CLIENT with registration and setup in required State grant management systems and will provide guidance regarding administrative documentation, including internal controls questionnaires (ICQ), risk assessments, and other compliance-related submissions required by the State administering agency.

4. NPSA will guide CLIENT through the State budget review, alignment, and allowability process and will assist in confirming that the approved federal application aligns with State-level requirements and funding conditions.

5. NPSA will provide guidance and administrative coordination related to completion and submission of Environmental and Historic Preservation (EHP) documentation, including assistance with required photographs, annotations, equipment specifications, and follow-up communication until approval is issued.

6. NPSA will review grant award documents and coordinate administrative execution of grant agreements and related documentation.

7. If designated by CLIENT as an authorized point of contact, NPSA will receive and review State communications related to the award and will provide CLIENT with summaries, signature pages, and required action items as appropriate.

8. NPSA will attend required trainings related to procurement, reimbursement, and financial compliance and will provide general guidance to CLIENT regarding applicable procurement standards and reimbursement requirements.

9. NPSA will assist CLIENT with registration or coordination with applicable State comptroller, treasury, or financial systems as required for reimbursement processing.

10. NPSA will provide ongoing coordination and communication support between CLIENT and the State administering agency during the pre-clearance period and will identify required action items and deadlines necessary to obtain clearance to begin committing grant funds.

11. These services apply only to the initial post-award compliance phase required before an award recipient is authorized to begin committing grant funds.

12. These services are advisory and administrative in nature and are intended to assist CLIENT in navigating State-specific post-award requirements. Because post-award procedures vary by State and may change annually, specific tasks, documentation requirements, and administrative processes may differ from those listed above.

13. NPSA's services under this section shall be deemed complete upon CLIENT's receipt of formal written clearance from the State authorizing the commitment of grant funds. At that time, all services described in this engagement shall be fully performed and earned unless otherwise governed by a separate written agreement.

14. Following receipt of such clearance, CLIENT may elect to engage NPSA under a separate written agreement for project management, solution implementation, procurement coordination, reimbursement coordination, documentation support, and grant close-out services.`}]},
  { id:"inh_liability", title:"Limits of Liability", roman:"II.",
    content:`1. CLIENT acknowledges that NPSA is not providing a vulnerability assessment and is simply providing licensed resources which CLIENT will vet. CLIENT is responsible for the final selection of security personnel and will contract with them independently of NPSA. CLIENT agrees NPSA will not be responsible for any recommendations or lack of recommendations made by security personnel.

2. Regardless of whether CLIENT chooses to complete a self-assessment or chooses to contract with a licensed security consultant, CLIENT will not hold NPSA responsible for any security assessments or decisions.

3. CLIENT acknowledges responsibility for choosing any security contractors or solutions providers. CLIENT is responsible for verifying the solutions are implemented satisfactorily and continue to operate satisfactorily. CLIENT will never hold NPSA responsible for any solutions failures.

4. CLIENT acknowledges that NPSA is not responsible for any security breaches or failures, or any harm caused by any bad actor irrespective of NPSA's role in this engagement.

5. With the exception of liability arising out of intentional and malicious acts by NPSA, CLIENT agrees NPSA shall never be held liable for an amount greater than the total fees paid by CLIENT to NPSA as part of this engagement. Furthermore, to the fullest extent possible, CLIENT shall not pursue NPSA for claims or damages that are covered (assuming compliance with the policy terms), in whole or part, by an insurance policy that insures CLIENT, and CLIENT waives any insurer or insured rights of subrogation related to said claim(s). In those instances, in which a loss could or are covered by insurance, CLIENT shall recover solely from insurance benefits or proceeds.

6. CLIENT agrees that in no event shall NPSA be liable for any indirect, incidental, consequential, special, exemplary, or punitive damages, including but not limited to loss of use, loss of revenue, loss of funding, or loss of goodwill, even if advised of the possibility of such damages.`},
  { id:"inh_conf", title:"Confidentiality", roman:"III.",
    content:`1. NPSA acknowledges and agrees that all CLIENT information shared by CLIENT as part of this engagement is owned by CLIENT. These documents are valuable assets of the CLIENT. Except for disclosure required to be made to advance the engagement and information which is a matter of public record, NPSA shall not use any information for the benefit of NPSA or any other person except with express written CLIENT consent.

2. CLIENT acknowledges and agrees that all NPSA information shared by NPSA as part of this engagement is owned by NPSA. This information includes but is not limited to this Engagement Letter, sales and marketing materials, cost information, and invoices. CLIENT agrees not to share this information with any person or organization except for the purposes of advancing this engagement.`},
  { id:"inh_resp", title:"Client Responsibilities", roman:"IV.",
    content:`1. CLIENT will attend all necessary CLIENT meetings whether in person or via web meeting and will provide reasonable advance notice in the event a meeting needs to be rescheduled.

2. CLIENT will provide all necessary information in a timely manner.

3. CLIENT agrees to make every possible effort to complete client-assigned tasks within a timely manner as set forth by NPSA.

4. CLIENT will be responsible for providing access to CLIENT facilities to security consultants, solutions providers, NPSA, and any other project stakeholder. CLIENT acknowledges that NPSA will perform all or most services under this engagement remotely. NPSA will not routinely access CLIENT's facilities. Any task requiring on-site presence, including vulnerability assessments or site reviews, will be completed by CLIENT's employees, contractors, law enforcement partners, or other third-party consultants at CLIENT's expense unless otherwise agreed in writing by both parties.

5. CLIENT acknowledges that while NPSA may assist with reviewing vendor proposals, pricing, and project documentation for purposes of grant compliance and administrative coordination, NPSA does not approve, recommend, warrant, or guarantee the performance, availability, quality, or work of any vendor, contractor, consultant, or solutions provider. CLIENT retains sole and final responsibility for all vendor selection decisions.

6. CLIENT agrees to provide written notice of any issues or problems that may arise within 48 hours of uncovering the issue.`},
  { id:"inh_comp", title:"Compensation", roman:"V.", content:"[COMP_BLOCK]" },
  { id:"inh_guar", title:"Guarantees of NPSA", roman:"VI.",
    content:`1. NPSA shall perform duties as agreed to by both parties and shall complete these duties in good faith.

2. CLIENT acknowledges that NPSA does not control federal, state, or third-party funding decisions and that no representations or guarantees are made regarding the approval, amount, or timing of any grant award.

3. [NOFO_CLAUSE]

4. If CLIENT applies for the [GRANT_YEAR] NSGP, the federal government provides a Notice of Funding, and CLIENT is not awarded any grant funding, no refund will be issued for fees collected as part of this engagement, but NPSA will provide the services outlined in this engagement letter for the next available NSGP Opportunity and waive the [UPFRONT_FEE] fee associated with COMPENSATION Section 1 above. In the event this occurs, and CLIENT is awarded grant funding, the fees associated with COMPENSATION Section 1 above will apply and all other provisions of this agreement will apply to the subsequent NSGP Opportunity.

5. If CLIENT elects not to submit an application for the [GRANT_YEAR] NSGP, or if CLIENT fails to provide required information, documentation, approvals, or cooperation necessary to complete the application within required deadlines, no refund shall be issued and all fees paid under this Agreement shall be considered earned.`},
  { id:"inh_cancel", title:"Cancellation", roman:"VII.",
    content:`1. Cancellation. The CLIENT may terminate this Agreement at any time, at will, and in the CLIENT's sole discretion.
   (b) If the CLIENT cancels this Agreement before NPSA has delivered a completed grant application ready for submission, any fees paid for services not yet performed shall be refunded, and any outstanding invoices for completed work shall remain due and payable.
   (c) If the CLIENT cancels this Agreement after NPSA has provided one or more completed grant applications ready for submission, the services under this Agreement shall be deemed fully earned and all fees paid shall be non-refundable.
   (d) If the CLIENT believes NPSA has materially breached this Agreement, the CLIENT must provide written notice to NPSA within forty-eight (48) hours of becoming aware of the alleged breach, and NPSA shall be given a reasonable opportunity to cure the breach prior to termination for cause.`},
  { id:"inh_other", title:"Other Terms and Conditions", roman:"VIII.",
    content:`1. NPSA is an independent contractor performing services for CLIENT and is not an agent or employee of CLIENT for any purpose.

2. CLIENT acknowledges that NPSA provides consulting and administrative services only and is not acting as a contractor, general contractor, installer, vendor, or solutions provider for any security equipment, systems, or services.

3. CLIENT agrees to provide NPSA with such information, necessary and reasonable, to perform the proposed services.

4. This Agreement does not obligate CLIENT to engage NPSA for post-award management and administration services.

5. Following execution of a grant award agreement with the State and receipt of required environmental and historical preservation (EHP) approval, CLIENT may select a post-award management and administration provider. CLIENT is free to select any qualified provider. NPSA may provide such services if CLIENT elects to engage NPSA.

6. In the event CLIENT elects to engage NPSA for post-award management and administration services, such services eligible for reimbursement under the grant will commence only after:
   (a) execution of a separate agreement between CLIENT and NPSA,
   (b) CLIENT's receipt of a fully executed grant award agreement from the State, and
   (c) receipt of required EHP approval.

7. This represents the entire proposal of NPSA. Any change to this document must be in writing and agreed upon by an authorized officer of NPSA. This proposal does not become a contract between CLIENT and NPSA until an officer of NPSA has accepted it and a signed copy returned to CLIENT.

8. The parties agree that any and all legal actions will be brought in the County of Winnebago, State of Illinois. To the extent necessary, the parties agree to submit to jurisdiction and waive any and all venue objections.`},
];

const DEFAULT_POST = [
  { id:"post_intro", title:"Introduction", roman:"",
    content:`Lynde Consulting LLC, d/b/a Nonprofit Security Advisors ("NPSA"), proposes to provide [CLIENT_NAME] ("CLIENT") with consulting, management, and administrative services associated with the implementation of an already-awarded [GRANT_YEAR] Nonprofit Security Grant Program ("NSGP") project.

This engagement applies exclusively to post-award grant management, procurement support, compliance, documentation, and close-out services related to CLIENT's NSGP award. NPSA will not provide grant writing, application development, or pre-award consulting under this agreement.`},
  { id:"post_scope", title:"Scope of Work", roman:"I.",
    content:`1. NPSA will provide post-award Management & Administration (M&A) support services, including but not limited to the following:

   (a) Grant & Compliance Review
       i.  Review of NSGP award documents, approved Investment Justification, allowable costs, and applicable federal and state compliance requirements.
       ii. Establishment of a compliance and reimbursement calendar aligned with grant deadlines.

   (b) Procurement Support
       i.  Leadership and guidance regarding FEMA, state, and federal procurement requirements.
       ii. Coordination of competitive procurement, including obtaining multiple qualified vendor proposals as required.
       iii. Preparation of proposal comparison summaries and written recommendations.
       iv. Review of vendor contracts for required grant-related terms and conditions.

   (c) Project Management & Oversight
       i.  Ongoing coordination with CLIENT and selected vendors.
       ii. Regular virtual status meetings and milestone tracking.
       iii. Support throughout implementation to ensure alignment with approved scope and budget.

   (d) Reimbursement & Documentation
       i.  Review of reimbursement packets prior to submission by CLIENT.
       ii. Ongoing documentation support to ensure audit-ready records.
       iii. Final close-out review and preparation of an organized, comprehensive project file for grant close-out and potential audit.

2. NPSA provides independent consulting and administrative services only and is not acting as a contractor, general contractor, or vendor for any physical security solutions.`},
  { id:"post_liability", title:"Limits of Liability", roman:"II.",
    content:`1. CLIENT acknowledges that NPSA does not provide security assessments, engineering services, or physical security installations. CLIENT is solely responsible for selecting, contracting with, supervising, and overseeing all vendors, contractors, security personnel, and solution providers. CLIENT further acknowledges that NPSA is not responsible for the design, evaluation, or determination of CLIENT's security plan and that NPSA's role is limited to providing implementation support and project management services for security solutions that CLIENT has independently determined to be appropriate and for which funding has been awarded.

2. CLIENT acknowledges responsibility for verifying that all security solutions are properly installed, implemented, maintained, and continue to operate in a satisfactory and effective manner.

3. CLIENT agrees that NPSA shall not be responsible for:
   (a) The acts, omissions, performance, or failures of any third-party vendors, contractors, security personnel, or solution providers;
   (b) The effectiveness of any implemented security solutions, including any determination or verification that such solutions have been properly installed, implemented, maintained, or continue to operate satisfactorily or effectively;
   (c) Any security incidents, breaches, losses, damages, or harm caused by third parties or bad actors.

4. Except in cases of intentional and malicious acts by NPSA, NPSA's total liability under this engagement shall not exceed the total fees paid by CLIENT to NPSA.

5. To the fullest extent permitted by law, CLIENT agrees as follows:
   (a) CLIENT shall not pursue NPSA for any claims or damages that are covered, in whole or in part, by an insurance policy maintained by CLIENT, assuming compliance with the applicable policy terms;
   (b) CLIENT expressly waives any insurer or insured rights of subrogation related to such claims;
   (c) In any instance where a loss is covered, or could be covered, by insurance, CLIENT agrees that recovery shall be sought solely from applicable insurance benefits or proceeds.

6. CLIENT agrees that in no event shall NPSA be liable for any indirect, incidental, consequential, special, exemplary, or punitive damages, including but not limited to loss of use, loss of revenue, loss of funding, or loss of goodwill, even if advised of the possibility of such damages.`},
  { id:"post_conf", title:"Confidentiality", roman:"III.",
    content:`1. NPSA acknowledges and agrees that all CLIENT information shared by CLIENT as part of this engagement is owned by CLIENT. These documents are valuable assets of the CLIENT. Except for disclosure required to be made to advance the engagement and information which is a matter of public record, NPSA shall not use any information for the benefit of NPSA or any other person except with express written CLIENT consent.

2. All NPSA materials, including this Engagement Letter, methodologies, tools, pricing, and documentation templates, remain the property of NPSA and may not be shared except as necessary to advance the engagement.`},
  { id:"post_resp", title:"Client Responsibilities", roman:"IV.",
    content:`1. CLIENT agrees to:
   (a) Participate in all required meetings, whether in person or via web-based meeting, and provide reasonable advance notice if a meeting must be rescheduled;
   (b) Provide all information, documentation, approvals, and responses reasonably requested by NPSA in a timely and accurate manner;
   (c) Make every possible effort to complete the project within twelve (12) months of the execution of this Engagement Letter and acknowledges that delays attributable to CLIENT, including delays in decision-making, procurement, access, or coordination, may extend the project timeline without relieving CLIENT of its obligations under this agreement;
   (d) Provide access to CLIENT facilities as required for vendor coordination, site visits, implementation activities, inspections, and project oversight;
   (e) Ensure that appropriate and informed CLIENT personnel are available to provide access, answer questions, facilitate coordination, and assist in overseeing the completion of solution implementation activities;
   (f) Contract directly with and be solely responsible for payment to all vendors, contractors, consultants, and service providers;
   (g) Provide written notice to NPSA of any issues, delays, concerns, or problems related to the project within forty-eight (48) hours of discovery.

2. CLIENT acknowledges and agrees that NPSA will perform all or most services under this engagement remotely and that on-site access for vendors, consultants, and other project stakeholders is the sole responsibility of CLIENT.

3. CLIENT acknowledges and agrees that while NPSA may assist with vendor identification, coordination, and procurement support, NPSA does not guarantee the availability, performance, quality, or work of any third parties, including vendors, contractors, consultants, or service providers, and that CLIENT retains sole and final responsibility for all vendor selection decisions.`},
  { id:"post_comp", title:"Compensation", roman:"V.",
    content:`1. Total Fixed Fee: [POST_FEE]

2. Payment Schedule:
   (a) [POST_PMT1]% due at signing of this Engagement Letter
   (b) [POST_PMT2]% due after completion of procurement activities
   (c) [POST_PMT3]% due after submission of final reimbursement documentation

3. Invoices are payable within 30 days. CLIENT is responsible for all payments to third-party vendors and service providers. NPSA does not advance or disburse funds on CLIENT's behalf.`},
  { id:"post_term", title:"Term & Termination", roman:"VI.",
    content:`1. Term. This Agreement shall commence upon execution by both parties and shall remain in effect until completion of the services described herein, unless earlier terminated.

2. Termination by CLIENT (Without Cause). CLIENT may terminate this Agreement at any time upon written notice to NPSA. In the event of termination, CLIENT shall be responsible for payment of all fees earned as of the effective date of termination. For purposes of this Agreement, "earned" shall mean:
   (a) The full amount of any completed milestone payment as described in the Compensation section; and
   (b) A prorated portion of the next unpaid milestone, calculated in good faith by NPSA based on the percentage of work completed toward that milestone at the time of termination, including but not limited to procurement coordination, vendor communications, documentation preparation, reimbursement preparation, monitoring activities, or close-out documentation; and
   (c) Any reasonable, documented out-of-pocket expenses incurred by NPSA in performance of services under this Agreement.

   Upon termination, NPSA shall cease performance of services except as reasonably necessary to provide an orderly transition of documentation already prepared.

3. Termination by NPSA (For Cause). NPSA may terminate upon written notice if CLIENT:
   (a) Fails to make required payments when due;
   (b) Fails to provide necessary documentation, access, or cooperation; or
   (c) Materially breaches any provision of this Agreement.

   All earned fees shall become immediately due and payable.`},
  { id:"post_other", title:"Other Terms", roman:"VII.",
    content:`1. NPSA is an independent contractor and not an employee or agent of CLIENT.

2. This Engagement Letter constitutes the entire agreement between the parties and supersedes all prior discussions.

3. This proposal does not become a contract between CLIENT and NPSA until an officer of NPSA has accepted it and signed a copy returned to CLIENT.

4. Any amendments must be in writing and signed by both parties.

5. The parties agree that any and all legal actions will be brought in the County of Winnebago, State of Illinois. To the extent necessary, the parties agree to submit to jurisdiction and waive any and all venue objections.`},
];

const defaultForm = {
  clientName:"NAME OF THE ORG", clientType:"Church",
  locations:[{name:"",address:"",city:"",state:"",zip:""}],
  contactName:"", contactTitle:"", contactEmail:"", contactPhone:"",
  maxAward:"200,000",
  grantYear:"2026", grantType:"Federal", engagementModel:"pre-only", pricingTier:"undiscounted",
  customFee:"",
  installments: false, installmentCount:2,
  installment1Pct:"50", installment1Label:"upon execution",
  installment2Pct:"50", installment2Label:"upon award notification",
  installment3Pct:"", installment3Label:"",
  optNofo:false, optStateSwitch:false, optPostAwardScope:false, optShortNotice:false,
  earlySigningDiscount:false, earlySigningDate:"March 15, 2026", earlySigningAmount:"500",
  postAwardFee:"1,000",
  customClause:"", polishedClause:"",
  // In-house pre-award fields
  inhEngagementModel:"inh-pre-only", inhPricingTier:"undiscounted", inhCustomFee:"",
  inhInstallments:false, inhInstallmentCount:2,
  inhInstallment1Pct:"50", inhInstallment1Label:"upon execution",
  inhInstallment2Pct:"50", inhInstallment2Label:"upon award notification",
  inhInstallment3Pct:"", inhInstallment3Label:"",
  inhOptNofo:false, inhOptStateSwitch:false, inhOptPostAwardScope:false, inhOptShortNotice:false,
  inhEarlySigningDiscount:false, inhEarlySigningDate:"March 15, 2026", inhEarlySigningAmount:"500",
  inhPostAwardFee:"1,000",
  inhCustomClause:"", inhPolishedClause:"",
  postFee:"7,000", postPmt1:"40", postPmt2:"30", postPmt3:"30", postGrantYear:String(new Date().getFullYear()),
  postCustomClause:"", postPolishedClause:"",
  // Grant Writer fields
  gwRecipientName:"", gwOrgName:"",
  gwDate:"",
  npsa1Name:"", npsa1Email:"", npsa1Phone:"", npsa2Selected:[],
  gwCcContacts:[],
  gwProfFee:"", gwPaymentTerms:"Net 30",
  gwGuar1:true, gwGuar2:false, gwGuar3:true, gwGuar4:false,
  gwGuar4Deadline:"",
};

function calcFees(model, tier, locs, optPostAwardScope, postAwardFee, customFee, earlySigningDiscount, earlySigningAmount) {
  const n = Math.min(parseInt(locs) || 1, 3); // cap at 3 for pricing table
  const discountPerLoc = earlySigningDiscount ? (parseFloat(String(earlySigningAmount).replace(/,/g,"")) || 0) : 0;
  const discount = discountPerLoc * n;
  const postAward = optPostAwardScope ? (parseFloat(String(postAwardFee).replace(/,/g,"")) || 0) * n : 0;
  const pricingKey = PRICING[model] ? model : model; // use model directly if it exists in PRICING
  const isPreOnly = model === "pre-only" || model === "inh-pre-only";
  if (tier === "custom") {
    const fee = Math.max(0, (parseFloat(String(customFee).replace(/,/g,"")) || 0) - discount);
    return { upfront: fee, baseUpfront: parseFloat(String(customFee).replace(/,/g,"")) || 0, discount, contingent: null, postAward: optPostAwardScope ? postAward : null, total: fee + postAward };
  }
  if (isPreOnly) {
    const pricing = PRICING[model] || PRICING["pre-only"];
    const base = pricing.tiers[tier]?.[n] || 0;
    const fee = Math.max(0, base - discount);
    return { upfront: fee, baseUpfront: base, discount, contingent: null, postAward: optPostAwardScope ? postAward : null, total: fee + postAward };
  } else {
    const pricing = PRICING[model] || PRICING["partial-contingency"];
    const base = pricing.tiers[tier]?.upfront[n] || 0;
    const up = Math.max(0, base - discount);
    const con = pricing.tiers[tier]?.contingent[n] || 0;
    return { upfront: up, baseUpfront: base, discount, contingent: con, postAward: optPostAwardScope ? postAward : null, total: up + con + postAward };
  }
}

function buildInstallmentText(installments, upfront) {
  if (!installments || !installments.count) return "";
  const { count, payments } = installments;
  const lines = payments.slice(0, count).map((p, i) => {
    const pct = parseFloat(p.pct) || 0;
    const amt = Math.round(upfront * pct / 100);
    return `       ${i+1}. ${fmt(amt)} (${pct}%) ${p.label||""}`.trimEnd();
  });
  return `\n\n   By agreement of the parties, this fee shall be paid in ${count === 2 ? "two (2)" : "three (3)"} installments as follows:\n${lines.join("\n")}`;
}

function buildCompBlock(model, fees, installments, grantYear, optPostAwardScope, postAwardFee, installmentCount, i1Pct, i1Label, i2Pct, i2Label, i3Pct, i3Label, earlySigningDiscount, earlySigningDate, earlySigningAmount) {
  const isInh = model.startsWith("inh-");
  const baseModel = isInh ? model.replace("inh-","") : model;
  let text = "";
  if (baseModel === "pre-only") {
    text = `1. Consulting Fee. CLIENT will pay NPSA ${fmt(fees.upfront)} upon execution of this Engagement Letter.${isInh ? "" : " This fee includes the combined costs of Nonprofit Security Advisors and our grant writing partners."}`;
    if (installments) text += buildInstallmentText(installments, fees.upfront);
    text += `\n   (a) If CLIENT provides written notice within twelve (12) months and prior to applying for a grant, this fee will be refunded less any other amounts due to NPSA in full within fifteen (15) days of written notice of CLIENT's intention to forgo grant submission.\n   (b) If CLIENT submits one or more grant application(s) or does not provide the timely notice in subsection (a), this fee will be considered earned and non-refundable regardless of whether the grant application is approved or funds are received.`;
    if (!isInh) text += `\n\n2. Third-Party Grant Writer. CLIENT will pay a third-party grant writer for grant writing services. These costs are not listed in this Engagement Letter because CLIENT must contract directly with the third party grant writer outside of NPSA's direction or control to remain in compliance with NSGP rules.`;
  } else {
    text = `1. Upfront Fee. CLIENT will pay NPSA ${fmt(fees.upfront)} upon execution of this Engagement Letter.`;
    if (fees.upfront === 0) text = `1. Upfront Fee. No upfront fee is due upon execution of this Engagement Letter under this engagement model.`;
    if (installments && fees.upfront > 0) text += buildInstallmentText(installments, fees.upfront);
    text += `\n   (a) If CLIENT provides written notice within twelve (12) months and prior to applying for a grant, this fee will be refunded in full within fifteen (15) days.\n   (b) If CLIENT submits one or more grant application(s) without providing the timely notice in subsection (a), this fee will be considered earned and non-refundable.`;
    text += `\n\n2. Contingent Fee. Upon notification of a grant award, CLIENT will pay NPSA an additional ${fmt(fees.contingent)}. This fee is due upon award notification and is not reimbursable by grant funds.`;
    if (!isInh) text += `\n\n3. Third-Party Grant Writer. CLIENT will pay a third-party grant writer for grant writing services directly, outside of NPSA's direction or control, to remain in compliance with NSGP rules.`;
  }

  if (earlySigningDiscount && earlySigningDate && earlySigningAmount) {
    text += `\n\n[EARLY_SIGNING_DISCOUNT:${earlySigningDate}:${fmt(fees.discount)}:${fmt(fees.baseUpfront)}]`;
  }

  // Post-Award Consulting and Administrative Support Fee block — shown when toggle is on
  if (optPostAwardScope) {
    text += `\n\nPost Award Consulting and Administrative Support Fee\n\n`;
    text += `1. In the event CLIENT is awarded funding under the ${grantYear} NSGP, CLIENT agrees to pay NPSA a fixed fee of ${fmt(fees.postAward)} for the Post Award Consulting and Administrative Support services described in this Engagement Letter.\n`;
    text += `2. This fee is not contingent upon the amount of funding awarded and is not calculated as a percentage of any grant award. Rather, this fee reflects the additional administrative workload required of NPSA upon award and covers services provided from award notification to receipt of formal written clearance from the State authorizing CLIENT to begin committing grant funds.\n`;
    text += `3. The Post Award Consulting and Administrative Support fee shall be due within thirty (30) days of CLIENT'S receipt of award notification.\n`;
    text += `4. All fees payable to NPSA under this Engagement Letter are non-reimbursable from grant funds and shall not be charged to, paid from, or otherwise included in any grant-funded budget or reimbursement request. CLIENT acknowledges that such fees are the sole financial responsibility of CLIENT.`;
  } else {
    text += `\n\nNote: None of the above costs are reimbursable from grant funds.`;
  }

  return text;
}

const SHARED_FIELDS = [
  { section:"Client Information" },
  { key:"clientName", label:"Organization Name" },
  { key:"clientType", label:"Organization Type", placeholder:"Church, School, Nonprofit" },
  { key:"contactName", label:"Primary Contact Name" },
  { key:"contactTitle", label:"Contact Title", placeholder:"Pastor, Executive Director" },
  { key:"contactEmail", label:"Email" },
  { key:"contactPhone", label:"Phone" },
];

const POST_FIELDS = [
  { section:"Grant Details" },
  { key:"postGrantYear", label:"Award Year", placeholder:"2024" },
  { section:"Compensation" },
  { key:"postFee", label:"Total Fixed Fee ($)", placeholder:"7,000" },
  { key:"postPmt1", label:"Payment 1 — At Signing (%)", placeholder:"40" },
  { key:"postPmt2", label:"Payment 2 — Post-Procurement (%)", placeholder:"30" },
  { key:"postPmt3", label:"Payment 3 — Final Reimbursement (%)", placeholder:"30" },
];

function useAI() {
  const [loading, setLoading] = useState(false);
  const polish = async (text: string, cb: (result: string) => void) => {
    if (!text.trim()) return;
    setLoading(true);
    try {
      const r = await fetch("/api/polish-clause", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }),
      });
      const d = await r.json();
      cb(d.polished || d.error || "Error generating clause.");
    } catch {
      cb("Error generating clause.");
    }
    setLoading(false);
  };
  return { polish, loading };
}

export default function LOEGenerator() {
  const [docTab, setDocTab] = useState("pre");
  const [mode, setMode] = useState("document");
  const [form, setForm] = useState(defaultForm);
  const [preSections, setPreSections] = useState(DEFAULT_PRE);
  const [postSections, setPostSections] = useState(DEFAULT_POST);
  const [inhSections, setInhSections] = useState(DEFAULT_INH);
  const [editingSections, setEditingSections] = useState(null);
  const [editingTab, setEditingTab] = useState("pre");
  const [activeEdit, setActiveEdit] = useState(null);
  const [savedBanner, setSavedBanner] = useState(false);
  const [confirmModal, setConfirmModal] = useState(null); // null or "pre"/"post"
  const [mgmtApprovalModal, setMgmtApprovalModal] = useState(false); // AI clause approval gate
  const previewRef = useRef();
  const { polish, loading } = useAI();
  const setF = (k,v) => setForm(f=>({...f,[k]:v}));

  const numLocs = (form.locations||[]).length || 1;
  const fees = calcFees(form.engagementModel, form.pricingTier, numLocs, form.optPostAwardScope, form.postAwardFee, form.customFee, form.earlySigningDiscount, form.earlySigningAmount);
  const inhFees = calcFees(form.inhEngagementModel, form.inhPricingTier, numLocs, form.inhOptPostAwardScope, form.inhPostAwardFee, form.inhCustomFee, form.inhEarlySigningDiscount, form.inhEarlySigningAmount);

  useEffect(()=>{
    try {
      const stored = typeof window !== "undefined" ? localStorage.getItem("npsa-pre-sections") : null;
      if(stored) {
        const parsed = JSON.parse(stored);
          // Ensure any section whose DEFAULT content has changed (e.g. new tokens) stays current
          const merged = parsed.map((s: { id?: string; content?: string; subsections?: { id?: string; content?: string }[] }) => {
            const def = DEFAULT_PRE.find(d => d.id === s.id);
            if (!def) return s;
            // If stored content is missing tokens that exist in the default, reset that section
            if (s.content && def.content) {
              const missingTokens = (def.content.match(/\[[A-Z_]+\]/g)||[]).filter(tok => !s.content.includes(tok));
              if (missingTokens.length > 0) return { ...s, content: def.content };
            }
            if (s.subsections && def.subsections) {
              const mergedSubs = s.subsections.map(sub => {
                const defSub = def.subsections.find(d => d.id === sub.id);
                if (!defSub) return sub;
                const missingTokens = (defSub.content.match(/\[[A-Z_]+\]/g)||[]).filter(tok => !sub.content.includes(tok));
                return missingTokens.length > 0 ? { ...sub, content: defSub.content } : sub;
              });
              return { ...s, subsections: mergedSubs };
            }
            return s;
          });
          setPreSections(merged);
        }
        const r2 = typeof window !== "undefined" ? localStorage.getItem("npsa-post-sections") : null;
        if (r2) setPostSections(JSON.parse(r2));
        const r3 = typeof window !== "undefined" ? localStorage.getItem("npsa-inh-sections") : null;
        if (r3) {
          const storedInh = JSON.parse(r3);
          const merged = storedInh.map((s: { id?: string; content?: string; subsections?: { id?: string; content?: string }[] }) => {
            const def = DEFAULT_INH.find(d => d.id === s.id);
            if (!def) return s;
            if (s.content && def.content) {
              const missingTokens = (def.content.match(/\[[A-Z_]+\]/g)||[]).filter(tok => !s.content.includes(tok));
              if (missingTokens.length > 0) return { ...s, content: def.content };
            }
            if (s.subsections && def.subsections) {
              const mergedSubs = s.subsections.map(sub => {
                const defSub = def.subsections.find(d => d.id === sub.id);
                if (!defSub) return sub;
                const missingTokens = (defSub.content.match(/\[[A-Z_]+\]/g)||[]).filter(tok => !sub.content.includes(tok));
                return missingTokens.length > 0 ? { ...sub, content: defSub.content } : sub;
              });
              return { ...s, subsections: mergedSubs };
            }
            return s;
          });
          setInhSections(merged);
        }
    } catch {}
  }, []);

  // Sync Grant Writer guarantee options with pre-award toggles
  useEffect(()=>{
    setForm(f => ({
      ...f,
      gwGuar2: f.optNofo ? true  : f.gwGuar2,
      gwGuar3: f.optNofo ? false : f.gwGuar3,
      gwGuar4: f.optShortNotice ? true : f.gwGuar4,
    }));
  },[form.optNofo, form.optShortNotice]);

  const enterTemplate = (tab) => {
    setEditingTab(tab);
    setEditingSections(JSON.parse(JSON.stringify(tab==="pre"?preSections:tab==="inh"?inhSections:postSections)));
    setActiveEdit(null); setMode("template");
  };
  const saveTemplate = () => {
    if(editingTab==="pre"){ setPreSections(editingSections); try{ localStorage.setItem("npsa-pre-sections",JSON.stringify(editingSections)); }catch{} }
    else if(editingTab==="inh"){ setInhSections(editingSections); try{ localStorage.setItem("npsa-inh-sections",JSON.stringify(editingSections)); }catch{} }
    else { setPostSections(editingSections); try{ localStorage.setItem("npsa-post-sections",JSON.stringify(editingSections)); }catch{} }
    setSavedBanner(true); setTimeout(()=>setSavedBanner(false),3000); setMode("document");
  };
  const resetTemplate = () => {
    if(!confirm("Reset to original default text?")) return;
    setEditingSections(JSON.parse(JSON.stringify(editingTab==="pre"?DEFAULT_PRE:editingTab==="inh"?DEFAULT_INH:DEFAULT_POST)));
    setActiveEdit(null);
  };
  const updateContent = (id,subId,val) => {
    setEditingSections(prev=>prev.map(s=>{
      if(s.id!==id) return s;
      if(subId&&s.subsections) return {...s,subsections:s.subsections.map(sub=>sub.id===subId?{...sub,content:val}:sub)};
      return {...s,content:val};
    }));
  };

  const today = new Date().toLocaleDateString("en-US",{year:"numeric",month:"long",day:"numeric"});
  const loc0 = (form.locations||[])[0]||{};
  const clientAddr = [loc0.address,loc0.city,loc0.state,loc0.zip].filter(Boolean).join(", ");

  const interpolatePre = (t) => {
    const installmentsObj = form.installments ? {
      count: parseInt(form.installmentCount) || 2,
      payments: [
        { pct: form.installment1Pct, label: form.installment1Label },
        { pct: form.installment2Pct, label: form.installment2Label },
        { pct: form.installment3Pct, label: form.installment3Label },
      ]
    } : null;
    const compBlock = buildCompBlock(form.engagementModel, fees, installmentsObj, form.grantYear, form.optPostAwardScope, form.postAwardFee, form.installmentCount, form.installment1Pct, form.installment1Label, form.installment2Pct, form.installment2Label, form.installment3Pct, form.installment3Label, form.earlySigningDiscount, form.earlySigningDate, form.earlySigningAmount);
    const nofoClause = form.optNofo
      ? `If the federal government does not issue a Notice of Funding Opportunity for a ${form.grantYear} NSGP, the CLIENT will have the sole right to choose either of the two options below.\n   (a) NPSA will refund the entire ${fmt(fees.upfront)} initial payment.\n   (b) NPSA will work with CLIENT to apply for the next available NSGP Opportunity, and the scope of the project will apply to that opportunity and waive the ${fmt(fees.upfront)} fee associated with COMPENSATION Section 1 above. In the event this occurs, and CLIENT is awarded grant funding, the fees associated with COMPENSATION Section 1 above will apply and all other provisions of this agreement will apply to the subsequent NSGP Opportunity.`
      : `If the federal government does not issue a Notice of Funding Opportunity for a ${form.grantYear} NSGP, NPSA will work with CLIENT to apply for the next available NSGP Opportunity, and the scope of the project will apply to that opportunity.`;
    const locList = (form.locations||[]).map((loc,i)=>{
      const parts=[loc.address,loc.city,loc.state,loc.zip].filter(Boolean).join(", ");
      const label=loc.name?`${loc.name}${parts?": ":""}`:"";
      return `   ${i+1}. ${label}${parts||"[Address TBD]"}`;
    }).join("\n");
    const numWords = ["zero","one","two","three","four","five","six","seven","eight","nine","ten"];
    const numAppsWord = numWords[numLocs] || String(numLocs);
    return t
      .replace(/\[CLIENT_NAME\]/g, form.clientName||"[CLIENT NAME]")
      .replace(/\[GRANT_YEAR\]/g, form.grantYear)
      .replace(/\[GRANT_TYPE\]/g, form.grantType)
      .replace(/\[GRANT_LABEL\]/g, `${form.grantYear} ${form.grantType} Nonprofit Security Grant Program ("NSGP")`)
      .replace(/\[MAX_AWARD\]/g, form.maxAward)
      .replace(/\[NUM_LOCATIONS\]/g, numLocs)
      .replace(/\[NUM_APPLICATIONS\]/g, numAppsWord)
      .replace(/\[APPLICATION_PLURAL\]/g, numLocs === 1 ? "application" : "applications")
      .replace(/\[CONSULTING_FEE\]/g, fmt(fees.upfront))
      .replace(/\[UPFRONT_FEE\]/g, fmt(fees.upfront))
      .replace(/\[LOCATION_LIST\]/g, locList)
      .replace(/\[NOFO_CLAUSE\]/g, nofoClause)
      .replace(/\[COMP_BLOCK\]/g, compBlock);
  };
  const interpolateInh = (t) => {
    const installmentsObj = form.inhInstallments ? {
      count: parseInt(form.inhInstallmentCount) || 2,
      payments: [
        { pct: form.inhInstallment1Pct, label: form.inhInstallment1Label },
        { pct: form.inhInstallment2Pct, label: form.inhInstallment2Label },
        { pct: form.inhInstallment3Pct, label: form.inhInstallment3Label },
      ]
    } : null;
    const compBlock = buildCompBlock(form.inhEngagementModel, inhFees, installmentsObj, form.grantYear, form.inhOptPostAwardScope, form.inhPostAwardFee, form.inhInstallmentCount, form.inhInstallment1Pct, form.inhInstallment1Label, form.inhInstallment2Pct, form.inhInstallment2Label, form.inhInstallment3Pct, form.inhInstallment3Label, form.inhEarlySigningDiscount, form.inhEarlySigningDate, form.inhEarlySigningAmount);
    const nofoClause = form.inhOptNofo
      ? `If the federal government does not issue a Notice of Funding Opportunity for a ${form.grantYear} NSGP, the CLIENT will have the sole right to choose either of the two options below.\n   (a) NPSA will refund the entire ${fmt(inhFees.upfront)} initial payment.\n   (b) NPSA will work with CLIENT to apply for the next available NSGP Opportunity, and the scope of the project will apply to that opportunity and waive the ${fmt(inhFees.upfront)} fee associated with COMPENSATION Section 1 above. In the event this occurs, and CLIENT is awarded grant funding, the fees associated with COMPENSATION Section 1 above will apply and all other provisions of this agreement will apply to the subsequent NSGP Opportunity.`
      : `If the federal government does not issue a Notice of Funding Opportunity for a ${form.grantYear} NSGP, NPSA will work with CLIENT to apply for the next available NSGP Opportunity, and the scope of the project will apply to that opportunity.`;
    const locList = (form.locations||[]).map((loc,i)=>{
      const parts=[loc.address,loc.city,loc.state,loc.zip].filter(Boolean).join(", ");
      const label=loc.name?`${loc.name}${parts?": ":""}`:"";
      return `   ${i+1}. ${label}${parts||"[Address TBD]"}`;
    }).join("\n");
    const numWords2 = ["zero","one","two","three","four","five","six","seven","eight","nine","ten"];
    const numAppsWord2 = numWords2[numLocs] || String(numLocs);
    return t
      .replace(/\[CLIENT_NAME\]/g, form.clientName||"[CLIENT NAME]")
      .replace(/\[GRANT_YEAR\]/g, form.grantYear)
      .replace(/\[GRANT_TYPE\]/g, form.grantType)
      .replace(/\[GRANT_LABEL\]/g, `${form.grantYear} ${form.grantType} Nonprofit Security Grant Program ("NSGP")`)
      .replace(/\[MAX_AWARD\]/g, form.maxAward)
      .replace(/\[NUM_LOCATIONS\]/g, numLocs)
      .replace(/\[NUM_APPLICATIONS\]/g, numAppsWord2)
      .replace(/\[APPLICATION_PLURAL\]/g, numLocs === 1 ? "application" : "applications")
      .replace(/\[CONSULTING_FEE\]/g, fmt(inhFees.upfront))
      .replace(/\[UPFRONT_FEE\]/g, fmt(inhFees.upfront))
      .replace(/\[LOCATION_LIST\]/g, locList)
      .replace(/\[NOFO_CLAUSE\]/g, nofoClause)
      .replace(/\[COMP_BLOCK\]/g, compBlock);
  };
  const interpolatePost = (t) => t
    .replace(/\[CLIENT_NAME\]/g, form.clientName||"[CLIENT NAME]")
    .replace(/\[GRANT_YEAR\]/g, form.postGrantYear)
    .replace(/\[POST_FEE\]/g, `$${form.postFee}`)
    .replace(/\[POST_PMT1\]/g, form.postPmt1)
    .replace(/\[POST_PMT2\]/g, form.postPmt2)
    .replace(/\[POST_PMT3\]/g, form.postPmt3);

  const getContent = (sections,id,subId,interp) => {
    const s = sections.find(x=>x.id===id); if(!s) return "";
    if(subId&&s.subsections){ const sub=s.subsections.find(x=>x.id===subId); return sub?interp(sub.content):""; }
    return s.content?interp(s.content):"";
  };

  const handlePrint = () => {
    const css = `
      body{font-family:Georgia,serif;font-size:11.5pt;line-height:1.7;color:#1a1a1a;margin:0;padding:0}
      .page{padding:72pt;max-width:8.5in;margin:0 auto}
      pre{white-space:pre-wrap;font-family:Georgia,serif;font-size:11pt;line-height:1.75;margin:0 0 8pt}
      div{font-family:Georgia,serif;font-size:11pt;line-height:1.75}
      div,span{box-sizing:border-box}
    `;
    const docTitle = isGw ? "Grant Writer Appointment Request - " : "Engagement Letter - ";
    const bodyHtml = previewRef.current.innerHTML;
    if (isGw) {
      // Open print dialog for PDF output
      const printHtml = "<html><head><title>" + docTitle + (form.clientName||"Client") + "<\/title><style>" + css + "@media print{@page{margin:72pt}body{margin:0}button{display:none!important}.no-print{display:none!important}}<\/style><\/head><body><div class=\"page\">" + bodyHtml + "<\/div><script>window.onload=function(){window.print();window.onafterprint=function(){window.close();};}<\/script><\/body><\/html>";
      const w = window.open("","_blank");
      if(w){ w.document.write(printHtml); w.document.close(); }
    } else {
      const html = "<html><head><title>" + docTitle + (form.clientName||"Client") + "<\/title><style>" + css + "<\/style><\/head><body><div class=\"page\">" + bodyHtml + "<\/div><\/body><\/html>";
      const blob = new Blob([html], {type:"text\/html"});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = docTitle + (form.clientName||"Client") + ".html";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }
  };

  // ── TEMPLATE EDITOR ────────────────────────────────────────────────────────
  if(mode==="template"){
    const TOKENS_PRE = ["[CLIENT_NAME]","[GRANT_YEAR]","[MAX_AWARD]","[NUM_LOCATIONS]","[NUM_APPLICATIONS]","[UPFRONT_FEE]","[POST_AWARD_FEE]","[NOFO_CLAUSE]","[COMP_BLOCK]","[LOCATION_LIST]"];
    const TOKENS_POST = ["[CLIENT_NAME]","[GRANT_YEAR]","[POST_FEE]","[POST_PMT1]","[POST_PMT2]","[POST_PMT3]"];
    const tokens = editingTab==="pre"||editingTab==="inh" ? TOKENS_PRE : TOKENS_POST;
    const TE_PRIMARY = "#1e3a5f", TE_BORDER = "#e5e7eb";
    return (
      <div style={{display:"flex",height:"100vh",fontFamily:"-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif",background:"#f5f5f5"}}>
        <div style={{width:260,background:"#fff",borderRight:`1px solid ${TE_BORDER}`,color:"#374151",overflowY:"auto",padding:"20px 16px",flexShrink:0}}>
          <div style={{fontWeight:700,fontSize:13,color:"#111",marginBottom:2}}>Template Editor</div>
          <div style={{fontSize:11,color:"#6b7280",marginBottom:14}}>Editing: <span style={{color:TE_PRIMARY,fontWeight:700}}>{editingTab==="pre"?"Pre-Award":editingTab==="inh"?"In-House Pre-Award":"Post-Award"}</span></div>
          {editingSections.map(s=>(
            <div key={s.id}>
              <button onClick={()=>setActiveEdit({sectionId:s.id,subId:null})}
                style={{width:"100%",textAlign:"left",background:activeEdit?.sectionId===s.id&&!activeEdit?.subId?TE_PRIMARY:"transparent",border:"none",color:activeEdit?.sectionId===s.id&&!activeEdit?.subId?"#fff":"#6b7280",padding:"8px 10px",borderRadius:8,fontSize:12,cursor:"pointer",marginBottom:2,fontWeight:600}}>
                {s.roman?`${s.roman} `:""}{s.title}
              </button>
              {s.subsections?.map(sub=>(
                <button key={sub.id} onClick={()=>setActiveEdit({sectionId:s.id,subId:sub.id})}
                  style={{width:"100%",textAlign:"left",background:activeEdit?.sectionId===s.id&&activeEdit?.subId===sub.id?"#2a3d60":"transparent",border:"none",color:activeEdit?.sectionId===s.id&&activeEdit?.subId===sub.id?"#fff":"#8892aa",padding:"6px 10px 6px 22px",borderRadius:6,fontSize:11,cursor:"pointer",marginBottom:2}}>
                  {sub.title}
                </button>
              ))}
            </div>
          ))}
          <div style={{marginTop:20,borderTop:`1px solid ${TE_BORDER}`,paddingTop:14,fontSize:10,color:"#6b7280"}}>Use tokens like {tokens[0]} where client-specific values appear.</div>
        </div>
        <div style={{flex:1,display:"flex",flexDirection:"column",overflow:"hidden"}}>
          <div style={{background:"#fff",borderBottom:`1px solid ${TE_BORDER}`,padding:"12px 24px",display:"flex",alignItems:"center",justifyContent:"space-between"}}>
            <div><span style={{fontWeight:700,fontSize:14,color:TE_PRIMARY}}>Editing Template</span><span style={{fontSize:12,color:"#6b7280",marginLeft:12}}>{editingTab==="pre"?"Pre-Award":editingTab==="inh"?"In-House Pre-Award":"Post-Award"}</span></div>
            <div style={{display:"flex",gap:10}}>
              <button onClick={resetTemplate} style={{background:"none",border:`1px solid ${TE_BORDER}`,borderRadius:12,padding:"7px 14px",fontSize:12,color:"#6b7280",cursor:"pointer"}}>Reset to Default</button>
              <button onClick={()=>setMode("document")} style={{background:"none",border:`1px solid ${TE_BORDER}`,borderRadius:12,padding:"7px 14px",fontSize:12,color:"#374151",cursor:"pointer"}}>Cancel</button>
              <button onClick={saveTemplate} style={{background:TE_PRIMARY,border:"none",borderRadius:12,padding:"7px 18px",fontSize:12,color:"#fff",fontWeight:700,cursor:"pointer"}}>💾 Save Template</button>
            </div>
          </div>
          <div style={{flex:1,overflowY:"auto",padding:"32px 40px"}}>
            {!activeEdit?(
              <div style={{textAlign:"center",color:"#aaa",marginTop:80,fontSize:14}}>← Select a section from the sidebar to edit</div>
            ):(() => {
              const sec=editingSections.find(s=>s.id===activeEdit.sectionId);
              const sub=activeEdit.subId?sec?.subsections?.find(s=>s.id===activeEdit.subId):null;
              const content=sub?sub.content:sec?.content||"";
              const title=sub?`${sec.roman} ${sec.title} — ${sub.title}`:`${sec.roman||""} ${sec.title}`.trim();
              return (
                <div style={{maxWidth:760,margin:"0 auto"}}>
                  <div style={{fontSize:13,fontWeight:700,color:TE_PRIMARY,marginBottom:6,textTransform:"uppercase",letterSpacing:1}}>{title}</div>
                  <textarea value={content} onChange={e=>updateContent(activeEdit.sectionId,activeEdit.subId,e.target.value)}
                    style={{width:"100%",minHeight:420,fontFamily:"Georgia,serif",fontSize:13,lineHeight:1.75,padding:"16px 20px",border:`1px solid ${TE_BORDER}`,borderRadius:12,resize:"vertical",outline:"none",color:"#1a1a1a",background:"#fff",boxSizing:"border-box"}}/>
                  <div style={{marginTop:10,display:"flex",gap:8,flexWrap:"wrap"}}>
                    {tokens.map(tok=>(
                      <button key={tok} onClick={()=>{
                        const ta=document.querySelector("textarea");
                        const s=ta.selectionStart,e=ta.selectionEnd;
                        updateContent(activeEdit.sectionId,activeEdit.subId,content.slice(0,s)+tok+content.slice(e));
                      }} style={{background:"#f3f4f6",border:`1px solid ${TE_BORDER}`,borderRadius:8,padding:"4px 10px",fontSize:11,color:TE_PRIMARY,cursor:"pointer",fontFamily:"monospace"}}>{tok}</button>
                    ))}
                  </div>
                </div>
              );
            })()}
          </div>
        </div>
      </div>
    );
  }

  // ── DOCUMENT MODE ──────────────────────────────────────────────────────────
  const isPre = docTab==="pre";
  const isGw = docTab==="gw";
  const isInh = docTab==="inh";
  const sections = isPre?preSections:isInh?inhSections:postSections;
  const interp = isPre?interpolatePre:isInh?interpolateInh:interpolatePost;
  const gc = (id,subId) => getContent(sections,id,subId,interp);

  // Renders section text with proper hanging indents for numbered/lettered list items
  const renderLines = (text) => {
    if (!text) return null;
    return text.split("\n").map((line, i) => {
      if (line.trim() === "") return <div key={i} style={{height:10}}/>;
      // Detect prefix patterns and compute indent levels
      // Level 0: "1." "2." "10." etc at start (after optional spaces)
      // Level 1: "(a)" "(b)" "a." "b." indented with spaces
      // Level 2: deeper sub-items
      const trimmed = line.trimStart();
      const leadSpaces = line.length - trimmed.length;

      // Match prefix: number+period, letter+period, (letter), (number)
      const prefixMatch = trimmed.match(/^(\d+\.|[a-z]\.|[A-Z]\.|[ivx]+\.|[IVX]+\.|\([a-z]\)|\([A-Z]\)|\(\d+\))\s+/);

      if (prefixMatch) {
        const prefix = prefixMatch[0]; // e.g. "1. " or "(a) "
        const rest = trimmed.slice(prefix.length);
        // Base indent from leading spaces (each space ≈ 0.55em in Georgia 13px)
        const baseIndent = leadSpaces * 0.55;
        // Hanging indent: prefix width in em (approx chars * 0.6em)
        const hangEm = prefix.length * 0.6;
        return (
          <div key={i} style={{
            fontFamily:"Georgia,serif", fontSize:13, lineHeight:1.75, color:"#222",
            display:"flex", alignItems:"flex-start",
            marginLeft:`${baseIndent}em`, marginBottom:2,
          }}>
            <span style={{flexShrink:0, width:`${hangEm}em`, display:"inline-block"}}>{prefix.trimEnd()}&nbsp;</span>
            <span style={{flex:1}}>{rest}</span>
          </div>
        );
      }

      // Plain line — render with leading space indentation
      return (
        <div key={i} style={{
          fontFamily:"Georgia,serif", fontSize:13, lineHeight:1.75, color:"#222",
          marginLeft:`${leadSpaces * 0.55}em`, marginBottom:2,
        }}>{trimmed}</div>
      );
    });
  };

  const SH = ({id}) => { const s=sections.find(x=>x.id===id); if(!s||!s.roman) return null;
    return <div style={{fontSize:11,fontWeight:700,textTransform:"uppercase",letterSpacing:2,color:"#1a4a6e",borderBottom:"2px solid #1a4a6e",paddingBottom:4,marginTop:30,marginBottom:10}}>{s.roman} {s.title}</div>; };
  const SubH = ({label}) => <div style={{fontSize:13,fontWeight:700,fontStyle:"italic",marginTop:14,marginBottom:6,color:"#333"}}>{label}</div>;
  const Body = ({id,subId}) => {
    const raw = gc(id,subId);
    const tokenRe = /\[EARLY_SIGNING_DISCOUNT:([^:]+):([^:]+):([^\]]+)\]/;
    const match = raw.match(tokenRe);
    if (!match) return <div style={{marginBottom:8}}>{renderLines(raw)}</div>;
    const [full, date, discAmt, baseFee] = match;
    const parts = raw.split(full);
    return <>
      <div style={{marginBottom:8}}>{renderLines(parts[0].trimEnd())}</div>
      <div style={{border:"1px solid #c0cfe8",borderRadius:4,background:"#f7f9fd",padding:"12px 16px",margin:"10px 0 8px",fontFamily:"Georgia,serif",fontSize:13,lineHeight:1.7,color:"#222"}}>
        <span style={{fontWeight:700,color:"#1a4a6e",fontSize:11,textTransform:"uppercase",letterSpacing:1,display:"block",marginBottom:5}}>Early Signing Discount</span>
        {`If this Agreement is executed on or before ${date}, a ${discAmt} discount will be applied to the above-mentioned ${baseFee} fee.`}
      </div>
      {parts[1]&&<div style={{marginBottom:8}}>{renderLines(parts[1].trimStart())}</div>}
    </>;
  };

  const NPSA_BG = "#f5f5f5";
  const NPSA_PRIMARY = "#1e3a5f";
  const NPSA_CARD = "#fff";
  const NPSA_BORDER = "#e5e7eb";

  return (
    <div style={{display:"flex",height:"100vh",minHeight:0,fontFamily:"-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif",background:NPSA_BG}}>
      {/* ── SIDEBAR (no duplicate branding — parent app provides it) ── */}
      <div style={{width:320,background:NPSA_CARD,borderRight:`1px solid ${NPSA_BORDER}`,color:"#374151",overflowY:"auto",padding:"20px 16px",flexShrink:0}}>
        <button onClick={()=>setConfirmModal(docTab)} style={{width:"100%",background:NPSA_PRIMARY,border:"none",borderRadius:12,padding:"10px 0",fontSize:12,color:"#fff",cursor:"pointer",marginBottom:14,fontWeight:600,display:isGw?"none":"block"}}>
          ✏️ Edit {isPre?"Pre-Award":isInh?"In-House Pre-Award":"Post-Award"} Template
        </button>
        {savedBanner&&<div style={{background:"#ecfdf5",border:"1px solid #a7f3d0",borderRadius:12,padding:"8px 12px",fontSize:12,color:"#059669",marginBottom:12}}>✓ Template saved</div>}

        {SHARED_FIELDS.map((f2,i)=>{
          if(f2.section) return <div key={i} style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>{f2.section}</div>;
          return (
            <div key={f2.key} style={{marginBottom:10}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>{f2.label}</label>
              <input value={form[f2.key]||""} onChange={e=>setF(f2.key,e.target.value)} placeholder={f2.placeholder||""}
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:12,padding:"8px 12px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          );
        })}

        {/* Locations — shared across all tabs */}
        <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Locations</div>
        {(form.locations||[]).map((loc,idx)=>(
          <div key={idx} style={{background:"#f9fafb",border:`1px solid ${NPSA_BORDER}`,borderRadius:12,padding:"12px",marginBottom:8}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
              <span style={{fontSize:11,color:"#6b7280",fontWeight:700}}>Location {idx+1}{idx===0?" (Primary)":""}</span>
              {idx>0&&<button onClick={()=>setF("locations",(form.locations||[]).filter((_,i)=>i!==idx))}
                style={{background:"none",border:"none",color:"#dc2626",fontSize:13,cursor:"pointer",padding:"0 2px",lineHeight:1}}>✕</button>}
            </div>
            {[{k:"name",ph:"Location / Site Name (optional)"},{k:"address",ph:"Street Address"},{k:"city",ph:"City"},{k:"state",ph:"State"},{k:"zip",ph:"ZIP"}].map(f2=>(
              <div key={f2.k} style={{marginBottom:6}}>
                <input value={loc[f2.k]||""} placeholder={f2.ph}
                  onChange={e=>{
                    const updated=[...(form.locations||[])];
                    updated[idx]={...updated[idx],[f2.k]:e.target.value};
                    setF("locations",updated);
                  }}
                  style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:8,padding:"6px 10px",color:"#374151",fontSize:11,boxSizing:"border-box",outline:"none"}}/>
              </div>
            ))}
          </div>
        ))}
        <button onClick={()=>setF("locations",[...(form.locations||[]),{name:"",address:"",city:"",state:"",zip:""}])}
          style={{width:"100%",background:"#fff",border:`1px dashed ${NPSA_BORDER}`,borderRadius:12,padding:"8px 0",fontSize:12,color:NPSA_PRIMARY,cursor:"pointer",marginBottom:14}}>
          + Add Location
        </button>

        {/* Pre-award specific */}
        {isPre&&<>
          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Grant Details</div>
          <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Grant Type</label>
          <div style={{display:"flex",gap:6,marginBottom:10}}>
            {["Federal","State"].map(t=>(
              <button key={t} onClick={()=>setF("grantType",t)}
                style={{flex:1,padding:"7px 0",borderRadius:6,border:"1px solid",fontSize:12,fontWeight:700,cursor:"pointer",
                  background:form.grantType===t?NPSA_PRIMARY:"#f9fafb",
                  borderColor:form.grantType===t?NPSA_PRIMARY:NPSA_BORDER,
                  color:form.grantType===t?"#fff":"#8892aa"}}>
                {t}
              </button>
            ))}
          </div>
          {[{key:"grantYear",label:"Grant Year",placeholder:"2026"},{key:"maxAward",label:"Max Award Per Location ($)",placeholder:"200,000"}].map(f2=>(
            <div key={f2.key} style={{marginBottom:10}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>{f2.label}</label>
              <input value={form[f2.key]||""} onChange={e=>setF(f2.key,e.target.value)} placeholder={f2.placeholder}
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          ))}

          {/* Fee Calculator */}
          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Fee Calculator</div>

          <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Engagement Model</label>
          <select value={form.engagementModel} onChange={e=>setF("engagementModel",e.target.value)}
            style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,marginBottom:10,outline:"none"}}>
            <option value="pre-only">Pre-Award Only</option>
            <option value="partial-contingency">Pre-Award + Partial Contingency</option>
          </select>

          <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Pricing Tier</label>
          <select value={form.pricingTier} onChange={e=>setF("pricingTier",e.target.value)}
            style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,marginBottom:10,outline:"none"}}>
            {Object.entries(TIER_LABELS).filter(([k])=>k!=="max").map(([k,v])=><option key={k} value={k}>{v}</option>)}
          </select>
          {form.pricingTier==="custom"&&(
            <div style={{marginBottom:10,marginTop:-4,paddingLeft:0}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Custom Fee Amount ($)</label>
              <input value={form.customFee||""} onChange={e=>setF("customFee",e.target.value)} placeholder="e.g. 5,000"
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          )}

          {/* Fee summary card */}
          <div style={{background:"#f9fafb",border:`1px solid ${NPSA_BORDER}`,borderRadius:8,padding:"12px 14px",marginBottom:10}}>
            <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:NPSA_PRIMARY,marginBottom:8}}>Fee Summary</div>
            <div style={{display:"flex",justifyContent:"space-between",fontSize:12,color:"#6b7280",marginBottom:4}}>
              <span>Upfront Fee</span><span style={{color:"#fff",fontWeight:600}}>{form.earlySigningDiscount&&fees.discount>0?<><span style={{textDecoration:"line-through",color:"#666",marginRight:6}}>{fmt(fees.baseUpfront)}</span>{fmt(fees.upfront)}</>:fmt(fees.upfront)}</span>
            </div>
            {form.earlySigningDiscount&&fees.discount>0&&(
              <div style={{display:"flex",justifyContent:"space-between",fontSize:11,color:"#e8a020",marginBottom:4}}>
                <span>Early Signing Discount</span><span style={{fontWeight:600}}>−{fmt(fees.discount)}</span>
              </div>
            )}
            {fees.contingent!==null&&(
              <div style={{display:"flex",justifyContent:"space-between",fontSize:12,color:"#6b7280",marginBottom:4}}>
                <span>Contingent Fee (on award)</span><span style={{color:"#fff",fontWeight:600}}>{fmt(fees.contingent)}</span>
              </div>
            )}
            {form.optPostAwardScope&&(
              <div style={{display:"flex",justifyContent:"space-between",fontSize:12,color:"#6b7280",marginBottom:4}}>
                <span>Post-Award Fee (on award){numLocs>1?` ×${numLocs}`:""}</span><span style={{color:"#fff",fontWeight:600}}>{fmt(fees.postAward)}</span>
              </div>
            )}
            <div style={{borderTop:"1px solid #2e4060",marginTop:6,paddingTop:6,display:"flex",justifyContent:"space-between",fontSize:13,fontWeight:700}}>
              <span style={{color:NPSA_PRIMARY}}>Total</span><span style={{color:NPSA_PRIMARY}}>{fmt(fees.total)}</span>
            </div>
          </div>

          {form.earlySigningDiscount&&(
            <div style={{background:"#201600",border:"1px solid #e8a020",borderRadius:8,padding:"12px 14px",marginBottom:10}}>
              <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#e8a020",marginBottom:10}}>Early Signing Discount</div>
              <div style={{marginBottom:8}}>
                <label style={{fontSize:11,color:"#c8a060",display:"block",marginBottom:2}}>Sign-By Date</label>
                <input value={form.earlySigningDate} onChange={e=>setF("earlySigningDate",e.target.value)} placeholder="March 15, 2026"
                  style={{width:"100%",background:"#2a1e00",border:"1px solid #e8a020",borderRadius:6,padding:"6px 10px",color:"#ffe8b0",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
              </div>
              <div>
                <label style={{fontSize:11,color:"#c8a060",display:"block",marginBottom:2}}>Discount Amount ($)</label>
                <input value={form.earlySigningAmount} onChange={e=>setF("earlySigningAmount",e.target.value)} placeholder="500"
                  style={{width:"100%",background:"#2a1e00",border:"1px solid #e8a020",borderRadius:6,padding:"6px 10px",color:"#ffe8b0",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
              </div>
              {fees.discount>0&&<div style={{fontSize:11,color:"#e8a020",marginTop:8,fontWeight:700}}>Discounted fee: {fmt(fees.upfront)} (saves {fmt(fees.discount)})</div>}
            </div>
          )}

          <label style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:form.installments?10:14,cursor:"pointer"}}>
            <input type="checkbox" checked={form.earlySigningDiscount} onChange={e=>setF("earlySigningDiscount",e.target.checked)} style={{accentColor:"#e8a020"}}/>
            Early signing discount
          </label>

          <label style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:form.installments?10:14,cursor:"pointer"}}>
            <input type="checkbox" checked={form.installments} onChange={e=>setF("installments",e.target.checked)} style={{accentColor:"#9aab2e"}}/>
            Allow installment payments
          </label>
          {form.installments&&(
            <div style={{background:"#f9fafb",border:`1px solid ${NPSA_BORDER}`,borderRadius:8,padding:"12px 14px",marginBottom:14}}>
              <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:NPSA_PRIMARY,marginBottom:10}}>Installment Schedule</div>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:4}}>Number of Payments</label>
              <div style={{display:"flex",gap:6,marginBottom:12}}>
                {[2,3].map(n=>(
                  <button key={n} onClick={()=>setF("installmentCount",n)}
                    style={{flex:1,padding:"7px 0",borderRadius:6,border:"1px solid",fontSize:12,fontWeight:700,cursor:"pointer",
                      background:form.installmentCount===n?NPSA_PRIMARY:"#f9fafb",
                      borderColor:form.installmentCount===n?NPSA_PRIMARY:NPSA_BORDER,
                      color:form.installmentCount===n?"#fff":"#8892aa"}}>
                    {n} Payments
                  </button>
                ))}
              </div>
              {[
                {pctKey:"installment1Pct",labelKey:"installment1Label",num:1},
                {pctKey:"installment2Pct",labelKey:"installment2Label",num:2},
                {pctKey:"installment3Pct",labelKey:"installment3Label",num:3,cond:form.installmentCount>=3},
              ].filter(r=>r.cond!==false).map(row=>{
                const pct = parseFloat(form[row.pctKey]) || 0;
                const amt = Math.round(fees.upfront * pct / 100);
                return (
                  <div key={row.num} style={{marginBottom:10,paddingBottom:10,borderBottom:"1px solid #1e3050"}}>
                    <div style={{fontSize:10,color:NPSA_PRIMARY,fontWeight:700,marginBottom:5}}>Payment {row.num}</div>
                    <div style={{display:"flex",gap:6,marginBottom:5}}>
                      <div style={{flex:"0 0 70px"}}>
                        <label style={{fontSize:10,color:"#6b7280",display:"block",marginBottom:2}}>%</label>
                        <input value={form[row.pctKey]} onChange={e=>setF(row.pctKey,e.target.value)} placeholder="50"
                          style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"5px 8px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
                      </div>
                      <div style={{flex:1}}>
                        <label style={{fontSize:10,color:"#6b7280",display:"block",marginBottom:2}}>Due When</label>
                        <input value={form[row.labelKey]} onChange={e=>setF(row.labelKey,e.target.value)} placeholder="upon execution"
                          style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"5px 8px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
                      </div>
                    </div>
                    {fees.upfront>0&&pct>0&&<div style={{fontSize:10,color:"#7edca8"}}>= {fmt(amt)}</div>}
                  </div>
                );
              })}
              {fees.upfront>0&&(()=>{
                const total = [form.installment1Pct,form.installment2Pct,form.installmentCount>=3?form.installment3Pct:"0"].slice(0,form.installmentCount).reduce((s,v)=>s+(parseFloat(v)||0),0);
                const ok = Math.abs(total-100)<0.01;
                return <div style={{fontSize:11,fontWeight:700,color:ok?"#22c55e":"#dc2626",marginTop:4}}>{ok?"✓ Percentages total 100%":`⚠ Total: ${total}% (must equal 100%)`}</div>;
              })()}
            </div>
          )}

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Optional Guarantees</div>
          {[{key:"optPostAwardScope",label:"Include Post-Award Consulting scope"},{key:"optNofo",label:"No NOFO"},{key:"optStateSwitch",label:"State NSGP Switch Option"}].map(o=>(
            <label key={o.key} style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:9,cursor:"pointer"}}>
              <input type="checkbox" checked={form[o.key]} onChange={e=>setF(o.key,e.target.checked)} style={{accentColor:"#9aab2e"}}/>
              {o.label}
            </label>
          ))}

          {/* Post-Award Fee input — shown when toggle is on */}
          {form.optPostAwardScope&&(
            <div style={{marginBottom:10,marginTop:2,paddingLeft:22}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Post-Award Fee ($)</label>
              <input value={form.postAwardFee||""} onChange={e=>setF("postAwardFee",e.target.value)} placeholder="1,000"
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          )}

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Optional Clauses</div>
          <label style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:9,cursor:"pointer"}}>
            <input type="checkbox" checked={form.optShortNotice} onChange={e=>setF("optShortNotice",e.target.checked)} style={{accentColor:"#9aab2e"}}/>
            Short-notice application
          </label>
        </>}

        {/* In-House Pre-Award specific */}
        {isInh&&<>
          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Grant Details</div>
          <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Grant Type</label>
          <div style={{display:"flex",gap:6,marginBottom:10}}>
            {["Federal","State"].map(t=>(
              <button key={t} onClick={()=>setF("grantType",t)}
                style={{flex:1,padding:"7px 0",borderRadius:6,border:"1px solid",fontSize:12,fontWeight:700,cursor:"pointer",
                  background:form.grantType===t?NPSA_PRIMARY:"#f9fafb",
                  borderColor:form.grantType===t?NPSA_PRIMARY:NPSA_BORDER,
                  color:form.grantType===t?"#fff":"#8892aa"}}>
                {t}
              </button>
            ))}
          </div>
          {[{key:"grantYear",label:"Grant Year",placeholder:"2026"},{key:"maxAward",label:"Max Award Per Location ($)",placeholder:"200,000"}].map(f2=>(
            <div key={f2.key} style={{marginBottom:10}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>{f2.label}</label>
              <input value={form[f2.key]||""} onChange={e=>setF(f2.key,e.target.value)} placeholder={f2.placeholder}
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          ))}

          {/* In-House Fee Calculator */}
          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Fee Calculator</div>
          <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Engagement Model</label>
          <select value={form.inhEngagementModel} onChange={e=>setF("inhEngagementModel",e.target.value)}
            style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,marginBottom:10,outline:"none"}}>
            <option value="inh-pre-only">Pre-Award Only</option>
            <option value="inh-partial-contingency">Pre-Award + Partial Contingency</option>
          </select>

          <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Pricing Tier</label>
          <select value={form.inhPricingTier} onChange={e=>setF("inhPricingTier",e.target.value)}
            style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,marginBottom:10,outline:"none"}}>
            {Object.entries(TIER_LABELS).map(([k,v])=><option key={k} value={k}>{v}</option>)}
          </select>
          {form.inhPricingTier==="custom"&&(
            <div style={{marginBottom:10,marginTop:-4}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Custom Fee Amount ($)</label>
              <input value={form.inhCustomFee||""} onChange={e=>setF("inhCustomFee",e.target.value)} placeholder="e.g. 5,000"
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          )}

          {/* In-House Fee summary card */}
          <div style={{background:"#f9fafb",border:`1px solid ${NPSA_BORDER}`,borderRadius:8,padding:"12px 14px",marginBottom:10}}>
            <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:NPSA_PRIMARY,marginBottom:8}}>Fee Summary</div>
            <div style={{display:"flex",justifyContent:"space-between",fontSize:12,color:"#6b7280",marginBottom:4}}>
              <span>Upfront Fee</span><span style={{color:"#fff",fontWeight:600}}>{form.inhEarlySigningDiscount&&inhFees.discount>0?<><span style={{textDecoration:"line-through",color:"#666",marginRight:6}}>{fmt(inhFees.baseUpfront)}</span>{fmt(inhFees.upfront)}</>:fmt(inhFees.upfront)}</span>
            </div>
            {form.inhEarlySigningDiscount&&inhFees.discount>0&&(
              <div style={{display:"flex",justifyContent:"space-between",fontSize:11,color:"#e8a020",marginBottom:4}}>
                <span>Early Signing Discount</span><span style={{fontWeight:600}}>−{fmt(inhFees.discount)}</span>
              </div>
            )}
            {inhFees.contingent!==null&&(
              <div style={{display:"flex",justifyContent:"space-between",fontSize:12,color:"#6b7280",marginBottom:4}}>
                <span>Contingent Fee (on award)</span><span style={{color:"#fff",fontWeight:600}}>{fmt(inhFees.contingent)}</span>
              </div>
            )}
            {form.inhOptPostAwardScope&&(
              <div style={{display:"flex",justifyContent:"space-between",fontSize:12,color:"#6b7280",marginBottom:4}}>
                <span>Post-Award Fee (on award){numLocs>1?` ×${numLocs}`:""}</span><span style={{color:"#fff",fontWeight:600}}>{fmt(inhFees.postAward)}</span>
              </div>
            )}
            <div style={{borderTop:"1px solid #2e4060",marginTop:6,paddingTop:6,display:"flex",justifyContent:"space-between",fontSize:13,fontWeight:700}}>
              <span style={{color:NPSA_PRIMARY}}>Total</span><span style={{color:NPSA_PRIMARY}}>{fmt(inhFees.total)}</span>
            </div>
          </div>

          {form.inhEarlySigningDiscount&&(
            <div style={{background:"#201600",border:"1px solid #e8a020",borderRadius:8,padding:"12px 14px",marginBottom:10}}>
              <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#e8a020",marginBottom:10}}>Early Signing Discount</div>
              <div style={{marginBottom:8}}>
                <label style={{fontSize:11,color:"#c8a060",display:"block",marginBottom:2}}>Sign-By Date</label>
                <input value={form.inhEarlySigningDate} onChange={e=>setF("inhEarlySigningDate",e.target.value)} placeholder="March 15, 2026"
                  style={{width:"100%",background:"#2a1e00",border:"1px solid #e8a020",borderRadius:6,padding:"6px 10px",color:"#ffe8b0",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
              </div>
              <div>
                <label style={{fontSize:11,color:"#c8a060",display:"block",marginBottom:2}}>Discount Amount ($)</label>
                <input value={form.inhEarlySigningAmount} onChange={e=>setF("inhEarlySigningAmount",e.target.value)} placeholder="500"
                  style={{width:"100%",background:"#2a1e00",border:"1px solid #e8a020",borderRadius:6,padding:"6px 10px",color:"#ffe8b0",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
              </div>
              {inhFees.discount>0&&<div style={{fontSize:11,color:"#e8a020",marginTop:8,fontWeight:700}}>Discounted fee: {fmt(inhFees.upfront)} (saves {fmt(inhFees.discount)})</div>}
            </div>
          )}

          <label style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:form.inhEarlySigningDiscount?10:14,cursor:"pointer"}}>
            <input type="checkbox" checked={form.inhEarlySigningDiscount} onChange={e=>setF("inhEarlySigningDiscount",e.target.checked)} style={{accentColor:"#e8a020"}}/>
            Early signing discount
          </label>

          <label style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:form.inhInstallments?10:14,cursor:"pointer"}}>
            <input type="checkbox" checked={form.inhInstallments} onChange={e=>setF("inhInstallments",e.target.checked)} style={{accentColor:"#9aab2e"}}/>
            Allow installment payments
          </label>
          {form.inhInstallments&&(
            <div style={{background:"#f9fafb",border:`1px solid ${NPSA_BORDER}`,borderRadius:8,padding:"12px 14px",marginBottom:14}}>
              <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:NPSA_PRIMARY,marginBottom:10}}>Installment Schedule</div>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:4}}>Number of Payments</label>
              <div style={{display:"flex",gap:6,marginBottom:12}}>
                {[2,3].map(n=>(
                  <button key={n} onClick={()=>setF("inhInstallmentCount",n)}
                    style={{flex:1,padding:"7px 0",borderRadius:6,border:"1px solid",fontSize:12,fontWeight:700,cursor:"pointer",
                      background:form.inhInstallmentCount===n?NPSA_PRIMARY:"#f9fafb",
                      borderColor:form.inhInstallmentCount===n?NPSA_PRIMARY:NPSA_BORDER,
                      color:form.inhInstallmentCount===n?"#fff":"#8892aa"}}>
                    {n} Payments
                  </button>
                ))}
              </div>
              {[
                {pctKey:"inhInstallment1Pct",labelKey:"inhInstallment1Label",num:1},
                {pctKey:"inhInstallment2Pct",labelKey:"inhInstallment2Label",num:2},
                {pctKey:"inhInstallment3Pct",labelKey:"inhInstallment3Label",num:3,cond:form.inhInstallmentCount>=3},
              ].filter(r=>r.cond!==false).map(row=>{
                const pct = parseFloat(form[row.pctKey]) || 0;
                const amt = Math.round(inhFees.upfront * pct / 100);
                return (
                  <div key={row.num} style={{marginBottom:10,paddingBottom:10,borderBottom:"1px solid #1e3050"}}>
                    <div style={{fontSize:10,color:NPSA_PRIMARY,fontWeight:700,marginBottom:5}}>Payment {row.num}</div>
                    <div style={{display:"flex",gap:6,marginBottom:5}}>
                      <div style={{flex:"0 0 70px"}}>
                        <label style={{fontSize:10,color:"#6b7280",display:"block",marginBottom:2}}>%</label>
                        <input value={form[row.pctKey]} onChange={e=>setF(row.pctKey,e.target.value)} placeholder="50"
                          style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"5px 8px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
                      </div>
                      <div style={{flex:1}}>
                        <label style={{fontSize:10,color:"#6b7280",display:"block",marginBottom:2}}>Due When</label>
                        <input value={form[row.labelKey]} onChange={e=>setF(row.labelKey,e.target.value)} placeholder="upon execution"
                          style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"5px 8px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
                      </div>
                    </div>
                    {inhFees.upfront>0&&pct>0&&<div style={{fontSize:10,color:"#7edca8"}}>= {fmt(amt)}</div>}
                  </div>
                );
              })}
              {inhFees.upfront>0&&(()=>{
                const total = [form.inhInstallment1Pct,form.inhInstallment2Pct,form.inhInstallmentCount>=3?form.inhInstallment3Pct:"0"].slice(0,form.inhInstallmentCount).reduce((s,v)=>s+(parseFloat(v)||0),0);
                const ok = Math.abs(total-100)<0.01;
                return <div style={{fontSize:11,fontWeight:700,color:ok?"#22c55e":"#dc2626",marginTop:4}}>{ok?"✓ Percentages total 100%":`⚠ Total: ${total}% (must equal 100%)`}</div>;
              })()}
            </div>
          )}

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Optional Guarantees</div>
          {[{key:"inhOptPostAwardScope",label:"Include Post-Award Consulting scope"},{key:"inhOptNofo",label:"No NOFO"},{key:"inhOptStateSwitch",label:"State NSGP Switch Option"}].map(o=>(
            <label key={o.key} style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:9,cursor:"pointer"}}>
              <input type="checkbox" checked={form[o.key]} onChange={e=>setF(o.key,e.target.checked)} style={{accentColor:"#9aab2e"}}/>
              {o.label}
            </label>
          ))}
          {form.inhOptPostAwardScope&&(
            <div style={{marginBottom:10,marginTop:2,paddingLeft:22}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Post-Award Fee ($)</label>
              <input value={form.inhPostAwardFee||""} onChange={e=>setF("inhPostAwardFee",e.target.value)} placeholder="1,000"
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          )}

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Optional Clauses</div>
          <label style={{display:"flex",alignItems:"center",gap:8,fontSize:12,color:"#6b7280",marginBottom:9,cursor:"pointer"}}>
            <input type="checkbox" checked={form.inhOptShortNotice} onChange={e=>setF("inhOptShortNotice",e.target.checked)} style={{accentColor:"#9aab2e"}}/>
            Short-notice application
          </label>
        </>}

        {/* Post-award fields */}
        {!isPre&&!isGw&&!isInh&&POST_FIELDS.map((f2,i)=>{
          if(f2.section) return <div key={i} style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>{f2.section}</div>;
          return (
            <div key={f2.key} style={{marginBottom:10}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>{f2.label}</label>
              <input value={form[f2.key]||""} onChange={e=>setF(f2.key,e.target.value)} placeholder={f2.placeholder||""}
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          );
        })}

        {/* Grant Writer sidebar fields */}
        {isGw&&<>
          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:4,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Grant Writer</div>
          {[{key:"gwRecipientName",label:"Grant Writer Name",placeholder:"Name"},{key:"gwOrgName",label:"Organization",placeholder:"Organization"}].map(f2=>(
            <div key={f2.key} style={{marginBottom:10}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>{f2.label}</label>
              <input value={form[f2.key]||""} onChange={e=>setF(f2.key,e.target.value)} placeholder={f2.placeholder}
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          ))}

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:12,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Requesting NPSA Consultant</div>
          <div style={{fontSize:11,color:"#6b7280",marginBottom:6}}>Select consultant</div>
          <div style={{display:"flex",flexDirection:"column",gap:5,marginBottom:10}}>
            {[
              {name:"Brad Lynde",     email:"brad@lyndeconsulting.com",           phone:"815-255-9556"},
              {name:"Chad Burgess",   email:"chad@nonprofitsecurityadvisors.com",  phone:"815-287-9339"},
              {name:"Josh Ullrich",   email:"josh@nonprofitsecurityadvisors.com",  phone:"815-608-3131"},
              {name:"Steven Timlick", email:"steven@nonprofitsecurityadvisors.com",phone:"815-255-9141"},
              {name:"Stuart Reese",   email:"stuart@nonprofitsecurityadvisors.com",phone:"815-550-5222"},
            ].map(rep=>{
              const active = form.npsa1Name===rep.name;
              return (
                <button key={rep.name} onClick={()=>{
                  setF("npsa1Name", rep.name);
                  setF("npsa1Email", rep.email);
                  setF("npsa1Phone", rep.phone);
                }}
                  style={{textAlign:"left",background:active?NPSA_PRIMARY:"#f9fafb",border:`1px solid ${active?NPSA_PRIMARY:NPSA_BORDER}`,borderRadius:6,padding:"7px 10px",color:active?"#fff":"#374151",fontSize:12,cursor:"pointer"}}>
                  <span style={{fontWeight:active?700:400}}>{rep.name}</span>
                </button>
              );
            })}
          </div>

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:12,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>CC Contacts</div>
          {(form.gwCcContacts||[]).map((cc,idx)=>(
            <div key={idx} style={{background:"#f9fafb",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"10px 10px 6px",marginBottom:8}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
                <span style={{fontSize:11,color:"#6b7280",fontWeight:700}}>Contact {idx+1}</span>
                <button onClick={()=>setF("gwCcContacts",(form.gwCcContacts||[]).filter((_,i)=>i!==idx))}
                  style={{background:"none",border:"none",color:"#dc2626",fontSize:13,cursor:"pointer",padding:"0 2px",lineHeight:1}}>✕</button>
              </div>
              {[{k:"name",ph:"Name"},{k:"title",ph:"Title"},{k:"phone",ph:"Phone"},{k:"email",ph:"Email"}].map(f2=>(
                <div key={f2.k} style={{marginBottom:6}}>
                  <input value={cc[f2.k]||""} placeholder={f2.ph}
                    onChange={e=>{
                      const updated=[...(form.gwCcContacts||[])];
                      updated[idx]={...updated[idx],[f2.k]:e.target.value};
                      setF("gwCcContacts",updated);
                    }}
                    style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"5px 8px",color:"#374151",fontSize:11,boxSizing:"border-box",outline:"none"}}/>
                </div>
              ))}
            </div>
          ))}
          <button onClick={()=>setF("gwCcContacts",[...(form.gwCcContacts||[]),{name:"",title:"",phone:"",email:""}])}
            style={{width:"100%",background:"#fff",border:`1px dashed ${NPSA_BORDER}`,borderRadius:6,padding:"7px 0",fontSize:11,color:NPSA_PRIMARY,cursor:"pointer",marginBottom:14}}>
            + Add CC Contact
          </button>

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:12,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Additional Meeting Attendees</div>
          <div style={{fontSize:11,color:"#6b7280",marginBottom:8,lineHeight:1.5}}>
            Organization contacts are auto-populated from above (primary contact + all CC contacts).
          </div>
          {(()=>{
            const orgAttendees = [
              form.contactName ? {name:form.contactName, email:form.contactEmail} : null,
              ...(form.gwCcContacts||[]).filter(c=>c.name).map(c=>({name:c.name,email:c.email})),
            ].filter(Boolean);
            if(!orgAttendees.length) return <div style={{fontSize:11,color:"#6b7280",fontStyle:"italic",marginBottom:10}}>Fill in client info above to see attendees here.</div>;
            return <div style={{background:"#f3f4f6",borderRadius:6,padding:"8px 10px",marginBottom:10}}>
              {orgAttendees.map((a,i)=>(
                <div key={i} style={{fontSize:11,color:"#6b7280",marginBottom:3}}>• {a.name}{a.email?` — ${a.email}`:""}</div>
              ))}
            </div>;
          })()}
          <div style={{fontSize:11,color:"#6b7280",marginBottom:6}}>Add Steven and/or Stuart to the meeting</div>
          <div style={{display:"flex",flexDirection:"column",gap:5,marginBottom:14}}>
            {[
              {name:"Steven Timlick", email:"steven@nonprofitsecurityadvisors.com", phone:"815-255-9141"},
              {name:"Stuart Reese",   email:"stuart@nonprofitsecurityadvisors.com",  phone:"815-550-5222"},
            ].map(rep=>{
              const isPrimary = form.npsa1Name===rep.name;
              if(isPrimary) return null;
              const selected = (form.npsa2Selected||[]).some(r=>r.name===rep.name);
              return (
                <button key={rep.name} onClick={()=>{
                  const cur = form.npsa2Selected||[];
                  if(selected){ setF("npsa2Selected", cur.filter(r=>r.name!==rep.name)); }
                  else { setF("npsa2Selected", [...cur, {name:rep.name,email:rep.email,phone:rep.phone}]); }
                }}
                  style={{textAlign:"left",background:selected?NPSA_PRIMARY:"#f9fafb",border:`1px solid ${selected?NPSA_PRIMARY:NPSA_BORDER}`,borderRadius:6,padding:"7px 10px",color:selected?"#fff":"#374151",fontSize:12,cursor:"pointer"}}>
                  <span style={{fontWeight:selected?700:400}}>{selected?"✓ ":""}{rep.name}</span>
                </button>
              );
            })}
          </div>

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:12,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Contract Terms</div>
          {[{key:"gwProfFee",label:"Professional Fee ($)",placeholder:"$$$"},{key:"gwPaymentTerms",label:"Payment Terms",placeholder:"Net 30"}].map(f2=>(
            <div key={f2.key} style={{marginBottom:10}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>{f2.label}</label>
              <input value={form[f2.key]||""} onChange={e=>setF(f2.key,e.target.value)} placeholder={f2.placeholder}
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          ))}

          <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:12,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>Guarantee Structure</div>
          {[
            {key:"gwGuar1", label:"1. One additional application at no additional fee if not awarded", locked:false, note:null},
            {key:"gwGuar2", label:"2. If no NOFO: apply work or refund within 10 business days",      locked:false, note:form.optNofo?"Auto-selected: No NOFO is on":null},
            {key:"gwGuar3", label:"3. If no NOFO: apply work to next available opportunity",           locked:false, note:form.optNofo?"Deselected: No NOFO is on":null},
            {key:"gwGuar4", label:"4. Commercially reasonable efforts to meet deadline",               locked:false, note:form.optShortNotice?"Auto-selected: Short-notice is on":null},
          ].map(o=>(
            <label key={o.key} style={{display:"flex",alignItems:"flex-start",gap:8,fontSize:12,color:o.locked?NPSA_PRIMARY:"#6b7280",marginBottom:8,cursor:o.locked?"default":"pointer",lineHeight:1.4,opacity:o.locked?0.8:1}}>
              <input type="checkbox" checked={form[o.key]} onChange={e=>!o.locked&&setF(o.key,e.target.checked)} disabled={o.locked} style={{accentColor:"#9aab2e",marginTop:2,flexShrink:0}}/>
              <span>
                {o.label}
                {o.locked&&<span style={{fontSize:10,color:NPSA_PRIMARY,marginLeft:6,fontStyle:"italic"}}>(always on)</span>}
                {o.note&&<span style={{fontSize:10,color:"#e8a020",display:"block",marginTop:1}}>{o.note}</span>}
              </span>
            </label>
          ))}
          {form.gwGuar4&&(
            <div style={{marginBottom:10,marginTop:-4,paddingLeft:20}}>
              <label style={{fontSize:11,color:"#6b7280",display:"block",marginBottom:2}}>Deadline</label>
              <input value={form.gwGuar4Deadline||""} onChange={e=>setF("gwGuar4Deadline",e.target.value)} placeholder="e.g. March 15, 2026"
                style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"6px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",outline:"none"}}/>
            </div>
          )}
          <div style={{fontSize:10,color:"#6b7280",fontStyle:"italic",marginTop:2,marginBottom:10,lineHeight:1.5}}>Note: Options 2 and 3 are mutually exclusive. Option 4 cannot be selected with 2 or 3.</div>
        </>}

        {/* AI Clause — hidden on Grant Writer tab */}
        {!isGw&&<>
        <div style={{fontSize:10,fontWeight:700,color:"#6b7280",letterSpacing:1,textTransform:"uppercase",marginTop:16,marginBottom:7,borderBottom:`1px solid ${NPSA_BORDER}`,paddingBottom:5}}>AI Custom Clause</div>
        <textarea value={isPre?form.customClause:form.postCustomClause} onChange={e=>setF(isPre?"customClause":"postCustomClause",e.target.value)}
          placeholder="Describe a clause in plain language..." rows={3}
          style={{width:"100%",background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:6,padding:"7px 10px",color:"#374151",fontSize:12,boxSizing:"border-box",resize:"vertical",outline:"none"}}/>
        <button onClick={()=>polish(isPre?form.customClause:form.postCustomClause,r=>setF(isPre?"polishedClause":"postPolishedClause",r))}
          disabled={loading||(isPre?!form.customClause.trim():!form.postCustomClause.trim())}
          style={{marginTop:7,width:"100%",background:loading?"#9ca3af":NPSA_PRIMARY,color:"#fff",border:"none",borderRadius:6,padding:"7px 0",fontSize:12,fontWeight:600,cursor:loading?"default":"pointer"}}>
          {loading?"Polishing...":"✨ Polish with AI"}
        </button>
        {(isPre?form.polishedClause:form.postPolishedClause)&&(
          <div style={{marginTop:9,background:"#1e3a2f",border:"1px solid #2d5c42",borderRadius:6,padding:10,fontSize:11,color:"#7edca8",lineHeight:1.6}}>
            <div style={{fontSize:10,color:"#4caf7d",marginBottom:3,fontWeight:700}}>POLISHED CLAUSE — PENDING APPROVAL</div>
            {isPre?form.polishedClause:form.postPolishedClause}
            <button onClick={()=>setMgmtApprovalModal(true)}
              style={{marginTop:10,width:"100%",background:"#1a4a2e",color:"#7edca8",border:"1px solid #2d5c42",borderRadius:6,padding:"7px 0",fontSize:12,fontWeight:700,cursor:"pointer"}}>
              ✓ Insert into Document
            </button>
          </div>
        )}
        </>}

        {/* Management Approval Modal */}
        {mgmtApprovalModal&&(
          <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.65)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:1000}}>
            <div style={{background:"#fff",border:`1px solid ${NPSA_BORDER}`,borderRadius:12,padding:"28px 32px",maxWidth:380,width:"90%",boxShadow:"0 8px 40px rgba(0,0,0,0.12)"}}>
              <div style={{fontSize:18,fontWeight:700,color:"#fff",marginBottom:8,textAlign:"center"}}>⚠ Management Approval Required</div>
              <div style={{fontSize:13,color:"#6b7280",marginBottom:24,textAlign:"center",lineHeight:1.6}}>Has this AI-generated clause been reviewed and confirmed by management before inserting it into the contract?</div>
              <div style={{display:"flex",gap:10}}>
                <button onClick={()=>{
                  setMgmtApprovalModal(false);
                  setF(isPre?"polishedClause":"postPolishedClause","");
                  setF(isPre?"customClause":"postCustomClause","");
                }} style={{flex:1,padding:"10px 0",borderRadius:8,border:`1px solid ${NPSA_BORDER}`,background:"#f9fafb",color:"#6b7280",fontSize:13,fontWeight:700,cursor:"pointer"}}>
                  No — Discard
                </button>
                <button onClick={()=>setMgmtApprovalModal(false)}
                  style={{flex:1,padding:"10px 0",borderRadius:8,border:"none",background:"#1a6e3a",color:"#fff",fontSize:13,fontWeight:700,cursor:"pointer"}}>
                  Yes — Approved
                </button>
              </div>
            </div>
          </div>
        )}

        <button onClick={handlePrint} style={{marginTop:20,width:"100%",background:NPSA_PRIMARY,color:"#fff",border:"none",borderRadius:12,padding:"10px 0",fontSize:13,fontWeight:700,cursor:"pointer"}}>
          {isGw ? "🖨 Print / Save as PDF" : "⬇ Download as HTML (open to print)"}
        </button>
      </div>

      {/* ── PREVIEW ── */}
      <div style={{flex:1,minWidth:0,minHeight:0,overflowY:"auto",padding:"24px 40px 40px",background:NPSA_BG}}>
        {/* Tabs — match NPSA Tools tab styling */}
        <div style={{maxWidth:800,margin:"0 auto",display:"flex",gap:0,background:NPSA_CARD,borderRadius:"16px 16px 0 0",border:`1px solid ${NPSA_BORDER}`,borderBottom:"none",overflow:"hidden"}}>
          {[{id:"pre",label:"Pre-Award"},{id:"inh",label:"Pre-Award (In-House)"},{id:"post",label:"Post-Award"},{id:"gw",label:"3rd Party Grant Writer"}].map((t,i)=>(
            <button key={t.id} onClick={()=>setDocTab(t.id)}
              style={{flex:1,padding:"14px 20px",fontSize:13,fontWeight:600,border:"none",
                cursor:"pointer",
                background:docTab===t.id?NPSA_PRIMARY:"transparent",
                color:docTab===t.id?"#fff":"#6b7280",
                borderBottom:docTab===t.id?"3px solid "+NPSA_PRIMARY:"3px solid transparent"}}>
              {t.label}
            </button>
          ))}
        </div>

        <div style={{maxWidth:800,margin:"0 auto",background:NPSA_CARD,border:`1px solid ${NPSA_BORDER}`,borderTop:"none",borderRadius:"0 0 16px 16px",padding:"64px 72px"}} ref={previewRef}>
          {/* Logo */}
          <div style={{textAlign:"center",borderBottom:"2.5px solid #1a4a6e",paddingBottom:16,marginBottom:20}}>
            <span style={{fontWeight:800,fontSize:36,color:"#1a4a6e",fontFamily:"Georgia,serif",display:"block",letterSpacing:-0.5}}>nonprofit</span>
            <span style={{fontWeight:400,fontSize:32,color:"#7a8c1e",fontFamily:"Georgia,serif",display:"block"}}>security advisors</span>
            <div style={{fontSize:10,color:"#888",marginTop:6,letterSpacing:0.5}}>Lynde Consulting LLC, DBA Nonprofit Security Advisors</div>
          </div>

          {/* Title */}
          <div style={{textAlign:"center",margin:"20px 0 6px"}}>
            <div style={{fontSize:17,fontWeight:700,letterSpacing:4,textTransform:"uppercase",color:"#1a1a1a",fontFamily:"Georgia,serif"}}>{isGw?"Contract & Appointment Request Form":"Engagement Letter"}</div>
            <div style={{fontSize:13,fontStyle:"italic",color:"#444",marginTop:4}}>
              {isGw?"3rd Party Grant Writer Engagement"
                :isInh?(form.inhEngagementModel==="inh-pre-only"?`Pre-Award ${form.grantType} NSGP Consulting & Grant Writing Services (In-House)`:`Pre-Award ${form.grantType} NSGP Consulting — With Partial Contingency (In-House)`)
                :isPre?(form.engagementModel==="pre-only"?`Pre-Award ${form.grantType} NSGP Consulting & Grant Writing Services`:`Pre-Award ${form.grantType} NSGP Consulting — With Partial Contingency`)
                :"Post-Award NSGP Management & Administration Services"}
            </div>
            <div style={{fontSize:11,color:"#666",marginTop:5}}>{today}</div>
          </div>

          {/* Parties — pre/post only */}
          {!isGw&&<div style={{border:"1px solid #c0c8d8",borderRadius:4,padding:"14px 20px",marginBottom:20,marginTop:20,background:"#f8f9fc",display:"flex",gap:40}}>
            <div style={{flex:1}}>
              <div style={{fontSize:9,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#666",marginBottom:4}}>Client</div>
              <div style={{fontSize:13,fontWeight:700,color:"#1a1a1a",fontFamily:"Georgia,serif"}}>{form.clientName||"[CLIENT NAME]"}</div>
              {form.clientType&&<div style={{fontSize:12,color:"#555"}}>{form.clientType}</div>}
              {clientAddr&&<div style={{fontSize:11,color:"#666",marginTop:2}}>{clientAddr}</div>}
              {form.contactName&&<div style={{fontSize:11,color:"#666",marginTop:2}}>{form.contactName}{form.contactTitle?`, ${form.contactTitle}`:""}</div>}
              {form.contactEmail&&<div style={{fontSize:11,color:"#666"}}>{form.contactEmail}</div>}
              {form.contactPhone&&<div style={{fontSize:11,color:"#666"}}>{form.contactPhone}</div>}
            </div>
            <div style={{width:1,background:"#c0c8d8"}}/>
            <div style={{flex:1}}>
              <div style={{fontSize:9,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#666",marginBottom:4}}>Consultant</div>
              <div style={{fontSize:13,fontWeight:700,color:"#1a1a1a",fontFamily:"Georgia,serif"}}>Nonprofit Security Advisors</div>
              <div style={{fontSize:12,color:"#555"}}>Lynde Consulting LLC</div>
              <div style={{fontSize:11,color:"#666",marginTop:2}}>Winnebago County, Illinois</div>
            </div>
          </div>}

          {/* Pre-Award Fee Summary Box */}
          {isPre&&(
            <div style={{border:"1px solid #1a4a6e",borderRadius:4,padding:"12px 18px",marginBottom:20,background:"#f4f7fb"}}>
              <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#1a4a6e",marginBottom:8}}>
                Fee Summary — {PRICING[form.engagementModel].label} · {numLocs} Location{numLocs>1?"s":""}
              </div>
              <div style={{display:"flex",gap:0,flexWrap:"wrap"}}>
                {form.installments ? (
                  [
                    {pct:form.installment1Pct, label:form.installment1Label, num:1},
                    {pct:form.installment2Pct, label:form.installment2Label, num:2},
                    ...(form.installmentCount>=3?[{pct:form.installment3Pct, label:form.installment3Label, num:3}]:[]),
                  ].map((p,i,arr)=>{
                    const pct = parseFloat(p.pct)||0;
                    const amt = Math.round(fees.upfront * pct / 100);
                    return (
                      <div key={p.num} style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16,minWidth:80}}>
                        <div style={{fontSize:10,color:"#888",marginBottom:2}}>Payment {p.num}{pct>0?` (${pct}%)`:""}</div>
                        <div style={{fontSize:16,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{pct>0?fmt(amt):"—"}</div>
                        <div style={{fontSize:10,color:"#888",marginTop:2}}>{p.label||"—"}</div>
                      </div>
                    );
                  })
                ) : (
                  <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                    <div style={{fontSize:10,color:"#888",marginBottom:2}}>Upfront Fee</div>
                    {fees.discount>0
                      ? <><div style={{fontSize:15,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif",textDecoration:"line-through",opacity:0.5}}>{fmt(fees.baseUpfront)}</div>
                          <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(fees.upfront)}</div>
                          <div style={{fontSize:10,color:"#e07030",marginTop:1,fontWeight:600}}>− {fmt(fees.discount)} early signing discount</div></>
                      : <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(fees.upfront)}</div>
                    }
                    <div style={{fontSize:10,color:"#888",marginTop:2}}>Due at signing</div>
                  </div>
                )}
                {fees.contingent!==null&&(
                  <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                    <div style={{fontSize:10,color:"#888",marginBottom:2}}>Contingent Fee</div>
                    <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(fees.contingent)}</div>
                    <div style={{fontSize:10,color:"#888",marginTop:2}}>Due after award notification</div>
                  </div>
                )}
                {form.optPostAwardScope&&(
                  <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                    <div style={{fontSize:10,color:"#888",marginBottom:2}}>Post-Award Fee{numLocs>1?` (×${numLocs})`:""}</div>
                    <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(fees.postAward)}</div>
                    <div style={{fontSize:10,color:"#888",marginTop:2}}>Due after award notification</div>
                  </div>
                )}
                <div style={{flex:1}}>
                  <div style={{fontSize:10,color:"#888",marginBottom:2}}>Total</div>
                  <div style={{fontSize:18,fontWeight:700,color:"#7a8c1e",fontFamily:"Georgia,serif"}}>{fmt(fees.total)}</div>
                  <div style={{fontSize:10,color:"#888",marginTop:2}}>Max grant: {fmt(parseInt(numLocs)*(parseFloat(String(form.maxAward).replace(/,/g,"")) || 200000))}</div>
                </div>
              </div>
            </div>
          )}

          {/* In-House Pre-Award Fee Summary Box */}
          {isInh&&(
            <div style={{border:"1px solid #1a4a6e",borderRadius:4,padding:"12px 18px",marginBottom:20,background:"#f4f7fb"}}>
              <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#1a4a6e",marginBottom:8}}>
                Fee Summary (In-House) — {PRICING[form.inhEngagementModel]?.label||"In-House"} · {numLocs} Location{numLocs>1?"s":""}
              </div>
              <div style={{display:"flex",gap:0,flexWrap:"wrap"}}>
                {form.inhInstallments ? (
                  [
                    {pct:form.inhInstallment1Pct, label:form.inhInstallment1Label, num:1},
                    {pct:form.inhInstallment2Pct, label:form.inhInstallment2Label, num:2},
                    ...(form.inhInstallmentCount>=3?[{pct:form.inhInstallment3Pct, label:form.inhInstallment3Label, num:3}]:[]),
                  ].map((p)=>{
                    const pct = parseFloat(p.pct)||0;
                    const amt = Math.round(inhFees.upfront * pct / 100);
                    return (
                      <div key={p.num} style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16,minWidth:80}}>
                        <div style={{fontSize:10,color:"#888",marginBottom:2}}>Payment {p.num}{pct>0?` (${pct}%)`:""}</div>
                        <div style={{fontSize:16,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{pct>0?fmt(amt):"—"}</div>
                        <div style={{fontSize:10,color:"#888",marginTop:2}}>{p.label||"—"}</div>
                      </div>
                    );
                  })
                ) : (
                  <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                    <div style={{fontSize:10,color:"#888",marginBottom:2}}>Upfront Fee</div>
                    {inhFees.discount>0
                      ? <><div style={{fontSize:15,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif",textDecoration:"line-through",opacity:0.5}}>{fmt(inhFees.baseUpfront)}</div>
                          <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(inhFees.upfront)}</div>
                          <div style={{fontSize:10,color:"#e07030",marginTop:1,fontWeight:600}}>− {fmt(inhFees.discount)} early signing discount</div></>
                      : <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(inhFees.upfront)}</div>
                    }
                    <div style={{fontSize:10,color:"#888",marginTop:2}}>Due at signing</div>
                  </div>
                )}
                {inhFees.contingent!==null&&(
                  <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                    <div style={{fontSize:10,color:"#888",marginBottom:2}}>Contingent Fee</div>
                    <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(inhFees.contingent)}</div>
                    <div style={{fontSize:10,color:"#888",marginTop:2}}>Due after award notification</div>
                  </div>
                )}
                {form.inhOptPostAwardScope&&(
                  <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                    <div style={{fontSize:10,color:"#888",marginBottom:2}}>Post-Award Fee{numLocs>1?` (×${numLocs})`:""}</div>
                    <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fmt(inhFees.postAward)}</div>
                    <div style={{fontSize:10,color:"#888",marginTop:2}}>Due after award notification</div>
                  </div>
                )}
                <div style={{flex:1}}>
                  <div style={{fontSize:10,color:"#888",marginBottom:2}}>Total</div>
                  <div style={{fontSize:18,fontWeight:700,color:"#7a8c1e",fontFamily:"Georgia,serif"}}>{fmt(inhFees.total)}</div>
                  <div style={{fontSize:10,color:"#888",marginTop:2}}>Max grant: {fmt(parseInt(numLocs)*(parseFloat(String(form.maxAward).replace(/,/g,"")) || 200000))}</div>
                </div>
              </div>
            </div>
          )}

          {/* Body sections */}
          {isPre&&<>
            <Body id="pre_intro"/>
            <SH id="pre_scope"/>
            <SubH label={form.optPostAwardScope
              ? (preSections.find(s=>s.id==="pre_scope")?.subsections?.[0]?.title||"A. Pre-Award Consulting")
              : "Pre-Award Consulting"} />
            <Body id="pre_scope" subId="pre_scope_pre"/>
            {form.optPostAwardScope&&<>
              <SubH label={preSections.find(s=>s.id==="pre_scope")?.subsections?.[1]?.title||"B. Post Award Consulting and Administrative Support"}/>
              <Body id="pre_scope" subId="pre_scope_post"/>
            </>}
            <SH id="pre_liability"/><Body id="pre_liability"/>
            <SH id="pre_conf"/><Body id="pre_conf"/>
            <SH id="pre_resp"/><Body id="pre_resp"/>
            <SH id="pre_comp"/><Body id="pre_comp"/>
            <SH id="pre_guar"/><Body id="pre_guar"/>
            {form.optStateSwitch&&(
              <div style={{border:"1px solid #c5d16a",borderRadius:4,background:"#f9fbf2",padding:"14px 18px",margin:"16px 0"}}>
                <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#5a6800",marginBottom:8}}>Additional Optional Guarantees</div>
                <p style={{fontSize:13,fontFamily:"Georgia,serif",lineHeight:1.7,margin:"4px 0"}}>• If CLIENT elects to transition from a Federal NSGP application to a State NSGP application, NPSA will accommodate such a change and apply all fees and services to the State NSGP opportunity.</p>
              </div>
            )}
            <SH id="pre_cancel"/><Body id="pre_cancel"/>
            <SH id="pre_other"/><Body id="pre_other"/>
            {form.optShortNotice&&(
              <>
                <div style={{fontSize:11,fontWeight:700,textTransform:"uppercase",letterSpacing:2,color:"#1a4a6e",borderBottom:"2px solid #1a4a6e",paddingBottom:4,marginTop:30,marginBottom:10}}>IX. Short-Notice Application Circumstances</div>
                {renderLines(
`1. CLIENT acknowledges that this engagement is being entered into with less than desirable notice.
2. NPSA commits to make all commercially reasonable efforts to position CLIENT to submit a compliant and well-written application.
3. If the application cannot be completed due to time constraints not caused by a material breach by NPSA;
   (a) No refund shall be issued; and
   (b) Work completed shall be applied to the next available NSGP opportunity at CLIENT's election; and
   (c) No additional fee under Compensation Section 1 shall apply; and
   (d) If CLIENT later receives funding, the fees described in Compensation Section 2 shall apply.`
                )}
              </>
            )}
            {form.polishedClause&&(
              <div style={{border:"1px solid #c0cfe8",borderRadius:4,background:"#f7f9fd",padding:"14px 18px",margin:"16px 0"}}>
                <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#1a4a6e",marginBottom:8}}>Additional Terms</div>
                <p style={{fontSize:13,fontFamily:"Georgia,serif",lineHeight:1.7,margin:0}}>{form.polishedClause}</p>
              </div>
            )}
          </>}

          {/* In-House Pre-Award body sections */}
          {isInh&&<>
            <Body id="inh_intro"/>
            <SH id="inh_scope"/>
            <SubH label={form.inhOptPostAwardScope
              ? (inhSections.find(s=>s.id==="inh_scope")?.subsections?.[0]?.title||"A. Pre-Award Consulting")
              : "Pre-Award Consulting"} />
            <Body id="inh_scope" subId="inh_scope_pre"/>
            {form.inhOptPostAwardScope&&<>
              <SubH label={inhSections.find(s=>s.id==="inh_scope")?.subsections?.[1]?.title||"B. Post Award Consulting and Administrative Support"}/>
              <Body id="inh_scope" subId="inh_scope_post"/>
            </>}
            <SH id="inh_liability"/><Body id="inh_liability"/>
            <SH id="inh_conf"/><Body id="inh_conf"/>
            <SH id="inh_resp"/><Body id="inh_resp"/>
            <SH id="inh_comp"/><Body id="inh_comp"/>
            <SH id="inh_guar"/><Body id="inh_guar"/>
            {form.inhOptStateSwitch&&(
              <div style={{border:"1px solid #c5d16a",borderRadius:4,background:"#f9fbf2",padding:"14px 18px",margin:"16px 0"}}>
                <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#5a6800",marginBottom:8}}>Additional Optional Guarantees</div>
                <p style={{fontSize:13,fontFamily:"Georgia,serif",lineHeight:1.7,margin:"4px 0"}}>• If CLIENT elects to transition from a Federal NSGP application to a State NSGP application, NPSA will accommodate such a change and apply all fees and services to the State NSGP opportunity.</p>
              </div>
            )}
            <SH id="inh_cancel"/><Body id="inh_cancel"/>
            <SH id="inh_other"/><Body id="inh_other"/>
            {form.inhOptShortNotice&&(
              <>
                <div style={{fontSize:11,fontWeight:700,textTransform:"uppercase",letterSpacing:2,color:"#1a4a6e",borderBottom:"2px solid #1a4a6e",paddingBottom:4,marginTop:30,marginBottom:10}}>IX. Short-Notice Application Circumstances</div>
                {renderLines(
`1. CLIENT acknowledges that this engagement is being entered into with less than desirable notice.
2. NPSA commits to make all commercially reasonable efforts to position CLIENT to submit a compliant and well-written application.
3. If the application cannot be completed due to time constraints not caused by a material breach by NPSA;
   (a) No refund shall be issued; and
   (b) Work completed shall be applied to the next available NSGP opportunity at CLIENT's election; and
   (c) No additional fee under Compensation Section 1 shall apply; and
   (d) If CLIENT later receives funding, the fees described in Compensation Section 2 shall apply.`
                )}
              </>
            )}
            {form.inhPolishedClause&&(
              <div style={{border:"1px solid #c0cfe8",borderRadius:4,background:"#f7f9fd",padding:"14px 18px",margin:"16px 0"}}>
                <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#1a4a6e",marginBottom:8}}>Additional Terms</div>
                <p style={{fontSize:13,fontFamily:"Georgia,serif",lineHeight:1.7,margin:0}}>{form.inhPolishedClause}</p>
              </div>
            )}
          </>}

          {!isPre&&!isGw&&!isInh&&<>
            {/* Post-Award Fee Summary Box */}
            {(()=>{
              const fee = parseFloat(String(form.postFee).replace(/,/g,"")) || 0;
              const p1 = parseFloat(form.postPmt1) || 0;
              const p2 = parseFloat(form.postPmt2) || 0;
              const p3 = parseFloat(form.postPmt3) || 0;
              const pmt1 = Math.round(fee * p1 / 100);
              const pmt2 = Math.round(fee * p2 / 100);
              const pmt3 = Math.round(fee * p3 / 100);
              return (
                <div style={{border:"1px solid #1a4a6e",borderRadius:4,padding:"12px 18px",marginBottom:20,background:"#f4f7fb"}}>
                  <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#1a4a6e",marginBottom:8}}>
                    Fee Summary — Post-Award M&A · {form.postGrantYear||new Date().getFullYear()}
                  </div>
                  <div style={{display:"flex",gap:0}}>
                    <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                      <div style={{fontSize:10,color:"#888",marginBottom:2}}>Payment 1 — At Signing{p1>0?` (${p1}%)`:""}</div>
                      <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fee>0&&p1>0?fmt(pmt1):"—"}</div>
                      <div style={{fontSize:10,color:"#888",marginTop:2}}>Due at signing</div>
                    </div>
                    <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                      <div style={{fontSize:10,color:"#888",marginBottom:2}}>Payment 2 — Post-Procurement{p2>0?` (${p2}%)`:""}</div>
                      <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fee>0&&p2>0?fmt(pmt2):"—"}</div>
                      <div style={{fontSize:10,color:"#888",marginTop:2}}>After procurement</div>
                    </div>
                    <div style={{flex:1,borderRight:"1px solid #c0cfe8",paddingRight:16,marginRight:16}}>
                      <div style={{fontSize:10,color:"#888",marginBottom:2}}>Payment 3 — Final Reimbursement{p3>0?` (${p3}%)`:""}</div>
                      <div style={{fontSize:18,fontWeight:700,color:"#1a4a6e",fontFamily:"Georgia,serif"}}>{fee>0&&p3>0?fmt(pmt3):"—"}</div>
                      <div style={{fontSize:10,color:"#888",marginTop:2}}>After final reimbursement</div>
                    </div>
                    <div style={{flex:1}}>
                      <div style={{fontSize:10,color:"#888",marginBottom:2}}>Total Fixed Fee</div>
                      <div style={{fontSize:18,fontWeight:700,color:"#7a8c1e",fontFamily:"Georgia,serif"}}>{fee>0?fmt(fee):"—"}</div>
                      <div style={{fontSize:10,color:"#888",marginTop:2}}>Fixed engagement fee</div>
                    </div>
                  </div>
                </div>
              );
            })()}
            <Body id="post_intro"/>
            <SH id="post_scope"/><Body id="post_scope"/>
            <SH id="post_liability"/><Body id="post_liability"/>
            <SH id="post_conf"/><Body id="post_conf"/>
            <SH id="post_resp"/><Body id="post_resp"/>
            <SH id="post_comp"/><Body id="post_comp"/>
            <SH id="post_term"/><Body id="post_term"/>
            <SH id="post_other"/><Body id="post_other"/>
            {form.postPolishedClause&&(
              <div style={{border:"1px solid #c0cfe8",borderRadius:4,background:"#f7f9fd",padding:"14px 18px",margin:"16px 0"}}>
                <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#1a4a6e",marginBottom:8}}>Additional Terms</div>
                <p style={{fontSize:13,fontFamily:"Georgia,serif",lineHeight:1.7,margin:0}}>{form.postPolishedClause}</p>
              </div>
            )}
          </>}

          {/* ── GRANT WRITER FORM ── */}
          {isGw&&(()=>{
            const loc0 = (form.locations||[])[0]||{};
            const clientAddr = [loc0.address,loc0.city,loc0.state,loc0.zip].filter(Boolean).join(", ");
            const F = ({label,value,placeholder}) => (
              <div style={{marginBottom:14}}>
                <div style={{fontSize:10,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#888",marginBottom:3}}>{label}</div>
                <div style={{fontSize:13,fontFamily:"Georgia,serif",color:value?"#1a1a1a":"#bbb",borderBottom:"1px solid #ccc",paddingBottom:4,minHeight:22}}>{value||placeholder||"—"}</div>
              </div>
            );
            const Row = ({children}) => <div style={{display:"flex",gap:32,marginBottom:0}}>{children}</div>;
            const Col = ({children,flex=1}) => <div style={{flex}}>{children}</div>;
            const SectionHead = ({num,title}) => (
              <div style={{fontSize:11,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#1a4a6e",borderBottom:"1.5px solid #1a4a6e",paddingBottom:4,marginTop:24,marginBottom:14}}>
                {num}. {title}
              </div>
            );
            const Check = ({checked,label}) => (
              <div style={{display:"flex",alignItems:"flex-start",gap:10,marginBottom:10,fontFamily:"Georgia,serif",fontSize:13,lineHeight:1.5}}>
                <span style={{fontSize:15,flexShrink:0,marginTop:1}}>{checked?"☒":"☐"}</span>
                <span style={{color:checked?"#1a1a1a":"#555"}}>{label}</span>
              </div>
            );
            return <>
              {/* Parties box — Grant Writer tab */}
              <div style={{border:"1px solid #c0c8d8",borderRadius:4,padding:"14px 20px",marginBottom:20,marginTop:20,background:"#f8f9fc",display:"flex",gap:40}}>
                <div style={{flex:1}}>
                  <div style={{fontSize:9,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#666",marginBottom:4}}>To: Grant Writer</div>
                  <div style={{fontSize:13,fontWeight:700,color:"#1a1a1a",fontFamily:"Georgia,serif"}}>{form.gwRecipientName||"[Grant Writer Name]"}</div>
                  {form.gwOrgName&&<div style={{fontSize:12,color:"#555"}}>{form.gwOrgName}</div>}
                </div>
                <div style={{width:1,background:"#c0c8d8"}}/>
                <div style={{flex:1}}>
                  <div style={{fontSize:9,fontWeight:700,textTransform:"uppercase",letterSpacing:1,color:"#666",marginBottom:4}}>From: Consultant</div>
                  {form.npsa1Name&&<div style={{fontSize:13,fontWeight:700,color:"#1a1a1a",fontFamily:"Georgia,serif"}}>{form.npsa1Name}</div>}
                  <div style={{fontSize:12,color:"#555"}}>Nonprofit Security Advisors</div>
                  {form.npsa1Email&&<div style={{fontSize:11,color:"#666",marginTop:2}}>{form.npsa1Email}</div>}
                  {form.npsa1Phone&&<div style={{fontSize:11,color:"#666"}}>{form.npsa1Phone}</div>}
                </div>
              </div>

              <div style={{fontSize:13,fontFamily:"Georgia,serif",color:"#555",fontStyle:"italic",marginBottom:20,marginTop:4}}>
                {form.npsa1Name||"[Consultant Name]"} is acting solely in his/her capacity as an authorized consultant of Nonprofit Security Advisors (NPSA).
              </div>

              <SectionHead num="1" title="Prospective Client Information"/>
              <F label="Organization" value={form.clientName} placeholder="Organization Name"/>
              <F label="Billing Address" value={clientAddr} placeholder="Address"/>

              <SectionHead num="2" title="Authorized Contract Signer"/>
              <Row>
                <Col><F label="Name" value={form.contactName} placeholder="Contact Name"/></Col>
                <Col><F label="Title" value={form.contactTitle} placeholder="Title"/></Col>
              </Row>
              <Row>
                <Col><F label="Phone" value={form.contactPhone} placeholder="Phone"/></Col>
                <Col><F label="Email" value={form.contactEmail} placeholder="Email"/></Col>
              </Row>

              {(()=>{
                let n = 2; // sections 1-2 are fixed above
                const hasCc = (form.gwCcContacts||[]).filter(c=>c.name||c.email).length>0;
                const ccNum   = hasCc ? ++n : null;
                const locsNum = ++n;
                const progNum = ++n;
                const termsNum = ++n;
                const guarNum  = ++n;
                const actionsNum = ++n;
                return <>
                  {hasCc&&<>
                    <SectionHead num={ccNum} title="Contract Carbon Copy Contact(s)"/>
                    {(form.gwCcContacts||[]).filter(c=>c.name||c.email).map((cc,idx)=>(
                      <Row key={idx}>
                        <Col><F label="Name" value={cc.name} placeholder="Name"/></Col>
                        <Col><F label="Title" value={cc.title} placeholder="Title"/></Col>
                        <Col><F label="Phone" value={cc.phone} placeholder="Phone"/></Col>
                        <Col><F label="Email" value={cc.email} placeholder="Email"/></Col>
                      </Row>
                    ))}
                  </>}

                  <SectionHead num={locsNum} title={`Location${numLocs>1?"s":""} (${numLocs})`}/>
                  {(form.locations||[]).map((loc,i)=>{
                    const addr=[loc.address,loc.city,loc.state,loc.zip].filter(Boolean).join(", ");
                    return (
                      <div key={i} style={{marginBottom:10}}>
                        {loc.name&&<F label={`Location ${i+1} Name`} value={loc.name} placeholder=""/>}
                        <F label={loc.name?`Location ${i+1} Address`:`Location ${i+1}`} value={addr} placeholder="Address"/>
                      </div>
                    );
                  })}

                  <SectionHead num={progNum} title="Grant Program"/>
                  <F label="Program" value={`${form.grantYear||""} ${form.grantType||""} NSGP`.trim()} placeholder="2026 Federal NSGP"/>

                  <SectionHead num={termsNum} title={`Requested Contract Terms for ${form.gwOrgName||"Grant Writer"} Preparation`}/>
                  <Row>
                    <Col><F label="Professional Fee" value={form.gwProfFee?`$${form.gwProfFee}`:""} placeholder="$0"/></Col>
                    <Col><F label="Payment Terms" value={form.gwPaymentTerms} placeholder="Net 30"/></Col>
                  </Row>

                  <SectionHead num={guarNum} title="Guarantee Structure (Select all that apply)"/>
                  <Check checked={form.gwGuar1} label="1. One additional application at no additional fee if not awarded (materially similar scope required)."/>
                  <Check checked={form.gwGuar2} label="2. If no NOFO: (a) Apply work to next comparable opportunity; or (b) Refund within 10 business days upon written request."/>
                  <Check checked={form.gwGuar3} label="3. If no NOFO is released, apply work to next available comparable opportunity."/>
                  <div style={{fontSize:12,fontFamily:"Georgia,serif",color:"#666",fontStyle:"italic",marginBottom:10,marginLeft:26}}>Note: Options 2 and 3 are mutually exclusive.</div>
                  <Check checked={form.gwGuar4} label={`4. Commercially reasonable efforts to meet deadline: ${form.gwGuar4Deadline||"____________"}. If not completed in time, apply work to next available opportunity.`}/>
                  <div style={{fontSize:12,fontFamily:"Georgia,serif",color:"#666",fontStyle:"italic",marginBottom:10,marginLeft:26}}>Note: If Option 4 is selected, Options 2 and 3 may not be selected.</div>
                  <div style={{fontSize:12,fontFamily:"Georgia,serif",color:"#555",fontStyle:"italic",marginTop:8,borderTop:"1px solid #ddd",paddingTop:10}}>NPSA provides recommendations only. Final terms remain subject solely to the Grant Writer's independent contract.</div>

                  <SectionHead num={actionsNum} title={`Requested Actions by ${form.gwOrgName||"Grant Writer"}`}/>
                </>;
              })()}
              <div style={{fontSize:13,fontFamily:"Georgia,serif",color:"#333",marginBottom:12}}>Upon receipt of this form, the Grant Writer is requested to:</div>
              {renderLines(`1. Send its services agreement directly to the Authorized Contract Signer${(form.gwCcContacts||[]).filter(c=>c.name).length>0?` (copy: ${(form.gwCcContacts||[]).filter(c=>c.name).map(c=>c.name).join(", ")})`:""}.
2. Provide its white paper and/or informational materials directly to the client.
3. Send a meeting invitation including:`)}
              <div style={{marginLeft:24,marginTop:8}}>
                <div style={{fontSize:12,fontWeight:700,color:"#1a4a6e",marginBottom:6,textTransform:"uppercase",letterSpacing:0.5}}>NPSA:</div>
                {form.npsa1Name&&<div style={{fontSize:13,fontFamily:"Georgia,serif",marginBottom:4}}>• {form.npsa1Name}{form.npsa1Email?` – ${form.npsa1Email}`:""}</div>}
                {(form.npsa2Selected||[]).map(r=>(
                  <div key={r.name} style={{fontSize:13,fontFamily:"Georgia,serif",marginBottom:4}}>• {r.name}{r.email?` – ${r.email}`:""}</div>
                ))}
                <div style={{fontSize:12,fontWeight:700,color:"#1a4a6e",marginBottom:6,textTransform:"uppercase",letterSpacing:0.5}}>{form.clientName||"CLIENT"}:</div>
                {[
                  form.contactName ? {name:form.contactName, email:form.contactEmail} : null,
                  ...(form.gwCcContacts||[]).filter(c=>c.name).map(c=>({name:c.name,email:c.email})),
                ].filter(Boolean).map((a,i)=>(
                  <div key={i} style={{fontSize:13,fontFamily:"Georgia,serif",marginBottom:4}}>• {a.name}{a.email?` – ${a.email}`:""}</div>
                ))}
              </div>

              {/* Disclosures */}
              <div style={{marginTop:28,borderTop:"2px solid #1a4a6e",paddingTop:16}}>
                <div style={{fontSize:13,fontWeight:700,color:"#1a1a1a",marginBottom:14,fontFamily:"Georgia,serif"}}>Disclosures</div>
                {(()=>{
                  const gw = form.gwOrgName||"the Grant Writer";
                  return [
                    {num:"1.", title:"Independent Entity & No Agency Disclosure", body:`NPSA and ${gw} are separate and independent entities. Neither is an agent, employee, partner, joint venturer, or representative of the other. This request does not create a partnership, joint venture, subcontracting relationship, exclusive arrangement, or agency relationship.`},
                    {num:"2.", title:"No Revenue Share / No Financial Interest Disclosure", body:`NPSA receives no commission, referral fee, revenue share, percentage of contract value, or contingent compensation related to the client's potential engagement of ${gw}. NPSA has no financial interest in whether the client retains ${gw}.`},
                    {num:"3.", title:"No Pre-Selection / Client Discretion Statement", body:`The client is under no obligation to retain ${gw} and may select any provider. This communication does not constitute vendor pre-selection, required designation, or procurement steering. Any engagement must be contracted directly between ${gw} and the client at the client's sole discretion.`},
                  ].map(d=>(
                    <div key={d.num} style={{marginBottom:16}}>
                      <div style={{fontSize:13,fontWeight:700,fontFamily:"Georgia,serif",color:"#1a1a1a",marginBottom:4}}>{d.num} {d.title}</div>
                      <div style={{fontSize:13,fontFamily:"Georgia,serif",color:"#333",lineHeight:1.7}}>{d.body}</div>
                    </div>
                  ));
                })()}
                <div style={{fontSize:13,fontFamily:"Georgia,serif",color:"#333",fontStyle:"italic",marginTop:12,borderTop:"1px solid #ddd",paddingTop:10}}>No services shall commence unless and until the Grant Writer executes a written agreement directly with the client.</div>
              </div>
            </>;
          })()}

          {/* Signature — pre/post only */}
          {!isGw&&<>
          <div style={{fontSize:11,fontWeight:700,textTransform:"uppercase",letterSpacing:2,color:"#1a4a6e",borderBottom:"2px solid #1a4a6e",paddingBottom:4,marginTop:30,marginBottom:14}}>Acknowledged and Agreed</div>
          <p style={{fontSize:13,fontFamily:"Georgia,serif",lineHeight:1.7,marginBottom:20}}>The undersigned parties hereby acknowledge and agree to the terms and conditions set forth in this Engagement Letter as of the date first written above.</p>
          <div style={{display:"flex",gap:48}}>
            {[{party:form.clientName||"CLIENT",sub:null,name:form.contactName,title:form.contactTitle},
              {party:"Lynde Consulting, LLC",sub:"DBA Nonprofit Security Advisors",name:"",title:""}
            ].map((p,i)=>(
              <div key={i} style={{flex:1}}>
                <div style={{fontWeight:700,fontSize:13,fontFamily:"Georgia,serif",marginBottom:2}}>{p.party}</div>
                {p.sub&&<div style={{fontSize:11,color:"#555",marginBottom:18}}>{p.sub}</div>}
                {!p.sub&&<div style={{marginBottom:18}}/>}
                {[["Signature",""],["Date",""],["Printed Name",p.name],["Title",p.title]].map(([lbl,val])=>(
                  <div key={lbl} style={{marginBottom:20}}>
                    <div style={{borderBottom:"1px solid #333",minHeight:24,paddingBottom:2,fontSize:13,color:val?"#111":"transparent",fontFamily:"Georgia,serif"}}>{val||"."}</div>
                    <div style={{fontSize:10,color:"#666",textTransform:"uppercase",letterSpacing:0.5,marginTop:3}}>{lbl}</div>
                  </div>
                ))}
              </div>
            ))}
          </div>
          </>}
          <div style={{marginTop:36,paddingTop:10,borderTop:"1px solid #ddd",textAlign:"center",fontSize:10,color:"#aaa"}}>
            Nonprofit Security Advisors &nbsp;•&nbsp; Lynde Consulting LLC &nbsp;•&nbsp; Winnebago County, Illinois &nbsp;•&nbsp; Confidential
          </div>
        </div>
      </div>

      {/* ── CONFIRM MODAL ── */}
      {confirmModal&&(
        <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.45)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:1000}}>
          <div style={{background:"#fff",borderRadius:10,padding:"32px 36px",maxWidth:380,width:"90%",boxShadow:"0 8px 40px rgba(0,0,0,0.22)",textAlign:"center"}}>
            <div style={{fontSize:22,marginBottom:12}}>✏️</div>
            <div style={{fontWeight:700,fontSize:16,color:NPSA_PRIMARY,marginBottom:8}}>Edit {confirmModal==="pre"?"Pre-Award":confirmModal==="inh"?"In-House Pre-Award":"Post-Award"} Template?</div>
            <div style={{fontSize:13,color:"#555",lineHeight:1.6,marginBottom:24}}>Are you sure you want to edit this template? Changes will affect all future documents generated from it.</div>
            <div style={{display:"flex",gap:12,justifyContent:"center"}}>
              <button onClick={()=>setConfirmModal(null)}
                style={{padding:"9px 24px",borderRadius:7,border:"1px solid #d0d5dd",background:"#fff",fontSize:13,fontWeight:600,color:"#444",cursor:"pointer"}}>
                Never Mind
              </button>
              <button onClick={()=>{ enterTemplate(confirmModal); setConfirmModal(null); }}
                style={{padding:"9px 24px",borderRadius:7,border:"none",background:NPSA_PRIMARY,fontSize:13,fontWeight:700,color:"#fff",cursor:"pointer"}}>
                Yes, Edit Template
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
